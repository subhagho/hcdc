package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.io.EncryptionHandler;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.core.state.HCdcStateManager;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.hcdc.agents.common.CDCDataConverter;
import ai.sapper.hcdc.agents.settings.NameNodeFileScannerSettings;
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
    private final HCdcStateManager stateManager;
    private final String name;
    private HCdcSchemaManager schemaManager;

    private ThreadPoolExecutor executorService;
    private NameNodeEnv env;
    private EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler;

    public NameNodeSchemaScanner(@NonNull HCdcStateManager stateManager, @NonNull String name) {
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
            NameNodeFileScannerSettings settings = (NameNodeFileScannerSettings) config.settings();
            connection = manger.getConnection(settings.getHdfsConnection(), HdfsConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("HDFS Connection not found. [name=%s]", settings.getHdfsConnection()));
            }
            if (!connection.isConnected()) connection.connect();
            if (settings.getEncryptorClass() != null) {
                encryptionHandler = settings.getEncryptorClass().getDeclaredConstructor().newInstance();
                encryptionHandler.init(config.config());
            }

            executorService = new ThreadPoolExecutor(settings.getThreads(),
                    settings.getThreads(),
                    1000,
                    TimeUnit.SECONDS,
                    new LinkedBlockingDeque<Runnable>());
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public NameNodeSchemaScanner withSchemaManager(@NonNull HCdcSchemaManager schemaManager) {
        this.schemaManager = schemaManager;
        return this;
    }

    public void run() throws Exception {
        try {
            NameNodeFileScannerSettings settings = (NameNodeFileScannerSettings) config.settings();
            List<DFSFileState> files = stateManager
                    .fileStateHelper()
                    .listFiles(null);
            if (files != null && !files.isEmpty()) {
                for (DFSFileState fs : files) {
                    long inode = fs.getFileInfo().getInodeId();
                    if (inode % settings.getShardCount() == settings.getShardId()) {
                        FileScannerTask task = new FileScannerTask(name)
                                .fileState(fs)
                                .schemaManager(schemaManager)
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
        private HCdcStateManager stateManager;
        private HCdcSchemaManager schemaManager;
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
                    CDCDataConverter converter = new CDCDataConverter(env.dbSource())
                            .withHdfsConnection(hdfsConnection)
                            .withSchemaManager(schemaManager);
                    if (encryptionHandler != null) {
                        converter.withEncryptionHandler(encryptionHandler);
                    }
                    SchemaEntity schemaEntity = new SchemaEntity(HCdcSchemaManager.DEFAULT_DOMAIN,
                            fileState.getFileInfo().getHdfsPath());
                    CDCDataConverter.ExtractSchemaResponse response = converter.extractSchema(fileState, schemaEntity);
                    if (response != null) {
                        if (response.schema() != null) {
                            String path = schemaManager.getSchemaEntityURI(schemaEntity);
                            if (!Strings.isNullOrEmpty(path)) {
                                fileState.getFileInfo().setSchemaURI(path);
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
                DefaultLogger.error(t.getLocalizedMessage());
            }
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class NameNodeFileScannerConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "scanner.files";


        public NameNodeFileScannerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, NameNodeFileScannerSettings.class);
        }

        public void read() throws ConfigurationException {
            super.read();
            try {
                NameNodeFileScannerSettings settings = (NameNodeFileScannerSettings) settings();
                if (settings.getShardCount() > 1) {
                    if (settings.getShardId() < 0) {
                        throw new ConfigurationException(
                                String.format("File Scanner Error: missing configuration. [name=%s]",
                                        NameNodeFileScannerSettings.Constants.CONFIG_SHARD_ID));
                    }
                }


                if (settings.getThreads() <= 0) {
                    throw new ConfigurationException(
                            String.format("File Scanner Error: missing configuration. [name=%s]",
                                    NameNodeFileScannerSettings.Constants.CONFIG_THREAD_COUNT));
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
