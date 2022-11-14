package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.io.EncryptionHandler;
import ai.sapper.cdc.core.schema.SchemaManager;
import ai.sapper.hcdc.agents.common.CDCDataConverter;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.model.DFSFileState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NameNodeSchemaScanner {
    private static final int MAX_EXECUTOR_QUEUE_SIZE = 32;
    private static final int THREAD_SLEEP_INTERVAL = 5000; // 5 secs.
    private NameNodeFileScannerConfig config;
    private HdfsConnection connection;
    private final ZkStateManager stateManager;
    private final String name;
    private SchemaManager schemaManager;

    private ThreadPoolExecutor executorService;
    private NameNodeEnv env;
    private EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler;

    public NameNodeSchemaScanner(@NonNull ZkStateManager stateManager, @NonNull String name) {
        this.stateManager = stateManager;
        this.name = name;
    }

    public NameNodeSchemaScanner init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                      @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            config = new NameNodeFileScannerConfig(xmlConfig);
            config.read();

            env = NameNodeEnv.get(name);
            Preconditions.checkNotNull(env);

            connection = manger.getConnection(config.hdfsConnection, HdfsConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("HDFS Connection not found. [name=%s]", config.hdfsConnection));
            }
            if (!connection.isConnected()) connection.connect();
            if (config.encryptorClass != null) {
                encryptionHandler = config.encryptorClass.getDeclaredConstructor().newInstance();
                encryptionHandler.init(config.config());
            }

            executorService = new ThreadPoolExecutor(config.threads,
                    config.threads,
                    1000,
                    TimeUnit.SECONDS,
                    new LinkedBlockingDeque<Runnable>());
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public NameNodeSchemaScanner withSchemaManager(@NonNull SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
        return this;
    }

    public void run() throws Exception {
        try {
            List<DFSFileState> files = stateManager
                    .fileStateHelper()
                    .listFiles(null);
            if (files != null && !files.isEmpty()) {
                for (DFSFileState fs : files) {
                    long inode = fs.getFileInfo().getInodeId();
                    if (inode % config.shardCount == config.shardId) {
                        FileScannerTask task = new FileScannerTask(name)
                                .fileState(fs)
                                .schemaManager(new SchemaManager(schemaManager))
                                .hdfsConnection(connection)
                                .encryptionHandler(encryptionHandler)
                                .stateManager(stateManager);
                        executorService.submit(task);
                        while (executorService.getQueue().size() > MAX_EXECUTOR_QUEUE_SIZE) {
                            Thread.sleep(THREAD_SLEEP_INTERVAL);
                        }
                    } else {
                        DefaultLogger.debug(env.LOG,
                                String.format("Skipped file: [%s]", fs.getFileInfo().getHdfsPath()));
                    }
                }
            }
            while (executorService.getQueue().size() > 0) {
                Thread.sleep(THREAD_SLEEP_INTERVAL);
            }
        } catch (Exception ex) {
            DefaultLogger.error(env.LOG, ex.getLocalizedMessage());
            DefaultLogger.stacktrace(env.LOG, ex);
            throw ex;
        } finally {
            executorService.shutdown();
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    private static class FileScannerTask implements Runnable {
        private final String name;
        private DFSFileState fileState;
        private HdfsConnection hdfsConnection;
        private ZkStateManager stateManager;
        private SchemaManager schemaManager;
        private EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler;

        public FileScannerTask(@NonNull String name) {
            this.name = name;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() {
            try {
                NameNodeEnv env = NameNodeEnv.get(name);
                Preconditions.checkNotNull(env);
                try {
                    CDCDataConverter converter = new CDCDataConverter()
                            .withHdfsConnection(hdfsConnection)
                            .withSchemaManager(schemaManager);
                    if (encryptionHandler != null) {
                        converter.withEncryptionHandler(encryptionHandler);
                    }
                    SchemaEntity schemaEntity = new SchemaEntity(SchemaManager.DEFAULT_DOMAIN,
                            fileState.getFileInfo().getHdfsPath());
                    CDCDataConverter.ExtractSchemaResponse response = converter.extractSchema(fileState, schemaEntity);
                    if (response != null) {
                        if (response.schema() != null) {
                            String path = schemaManager().schemaPath(schemaEntity);
                            if (!Strings.isNullOrEmpty(path)) {
                                fileState.getFileInfo().setSchemaLocation(path);
                            }
                        }
                        fileState.getFileInfo().setFileType(response.fileType());
                        try (DistributedLock lock = NameNodeEnv.get(name).globalLock()) {
                            lock.lock();
                            try {
                                stateManager.fileStateHelper().update(fileState);
                            } finally {
                                lock.unlock();
                            }
                        }
                    }
                } catch (Exception ex) {
                    DefaultLogger.error(env.LOG, ex.getLocalizedMessage());
                    DefaultLogger.stacktrace(env.LOG, ex);
                }
            } catch (Throwable t) {
                DefaultLogger.stacktrace(t);
                DefaultLogger.LOGGER.error(t.getLocalizedMessage());
            }
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class NameNodeFileScannerConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "scanner.files";

        public static class Constants {
            public static final String CONFIG_HDFS_CONN = "hdfs";
            public static final String CONFIG_SHARD_PATH = "shards";
            public static final String CONFIG_SHARD_COUNT = String.format("%s.count", CONFIG_SHARD_PATH);
            public static final String CONFIG_SHARD_ID = String.format("%s.id", CONFIG_SHARD_PATH);
            public static final String CONFIG_THREAD_COUNT = "threads";
            public static final String CONFIG_ENCRYPTOR_CLASS = String.format("%s.class", EncryptionHandler.__CONFIG_PATH);
        }

        private String hdfsConnection;
        private HierarchicalConfiguration<ImmutableNode> fsConfig;
        private int shardCount = 1;
        private int shardId = 0;
        private int threads = 1;
        private Class<? extends EncryptionHandler<ByteBuffer, ByteBuffer>> encryptorClass;

        public NameNodeFileScannerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        @SuppressWarnings("unchecked")
        public void read() throws ConfigurationException {
            try {
                hdfsConnection = get().getString(Constants.CONFIG_HDFS_CONN);
                if (Strings.isNullOrEmpty(hdfsConnection)) {
                    throw new ConfigurationException(
                            String.format("File Scanner Error: missing configuration. [name=%s]",
                                    Constants.CONFIG_HDFS_CONN));
                }

                if (get().containsKey(Constants.CONFIG_SHARD_PATH)) {
                    shardCount = get().getInt(Constants.CONFIG_SHARD_COUNT);
                    if (shardCount < 0) {
                        throw new ConfigurationException(
                                String.format("File Scanner Error: missing configuration. [name=%s]",
                                        Constants.CONFIG_SHARD_COUNT));
                    }
                    if (shardCount > 0) {
                        shardId = get().getInt(Constants.CONFIG_SHARD_ID);
                        if (shardId < 0) {
                            throw new ConfigurationException(
                                    String.format("File Scanner Error: missing configuration. [name=%s]",
                                            Constants.CONFIG_SHARD_ID));
                        }
                    }
                }
                if (get().containsKey(Constants.CONFIG_THREAD_COUNT)) {
                    threads = get().getInt(Constants.CONFIG_THREAD_COUNT);
                    if (threads < 0) {
                        throw new ConfigurationException(
                                String.format("File Scanner Error: missing configuration. [name=%s]",
                                        Constants.CONFIG_THREAD_COUNT));
                    }
                }
                if (checkIfNodeExists(get(), EncryptionHandler.__CONFIG_PATH)) {
                    String s = get().getString(EntityChangeDeltaReader.EntityChangeDeltaReaderConfig.Constants.CONFIG_ENCRYPTOR_CLASS);
                    checkStringValue(s, getClass(), EntityChangeDeltaReader.EntityChangeDeltaReaderConfig.Constants.CONFIG_ENCRYPTOR_CLASS);
                    encryptorClass = (Class<? extends EncryptionHandler<ByteBuffer, ByteBuffer>>) Class.forName(s);
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
