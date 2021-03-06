package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.CDCDataConverter;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.model.DFSFileState;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NameNodeFileScanner {
    private static final int MAX_EXECUTOR_QUEUE_SIZE = 32;
    private static final int THREAD_SLEEP_INTERVAL = 5000; // 5 secs.
    private FileSystem fs;
    private NameNodeFileScannerConfig config;
    private HdfsConnection connection;
    private FileSystem.FileSystemMocker fileSystemMocker;
    private ZkStateManager stateManager;
    private ThreadPoolExecutor executorService;
    private PathInfo schemaDir;

    public NameNodeFileScanner(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
    }

    public NameNodeFileScanner withMockFileSystem(@NonNull FileSystem.FileSystemMocker fileSystemMocker) {
        this.fileSystemMocker = fileSystemMocker;
        return this;
    }

    public NameNodeFileScanner init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                    @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            config = new NameNodeFileScannerConfig(xmlConfig);
            config.read();

            connection = manger.getConnection(config.hdfsConnection, HdfsConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("HDFS Connection not found. [name=%s]", config.hdfsConnection));
            }
            if (!connection.isConnected()) connection.connect();

            if (fileSystemMocker == null) {
                Class<? extends FileSystem> fsc = (Class<? extends FileSystem>) Class.forName(config.fsType);
                fs = fsc.newInstance();
                fs.init(config.config(), FileDeltaProcessor.FileDeltaProcessorConfig.Constants.CONFIG_PATH_FS);
            } else {
                fs = fileSystemMocker.create(config.config());
            }
            schemaDir = fs.get(config.schemaDir);
            if (!schemaDir.exists()) {
                fs.mkdirs(schemaDir);
            }

            executorService = new ThreadPoolExecutor(1,
                    config.threads,
                    1000,
                    TimeUnit.SECONDS,
                    new LinkedBlockingDeque<Runnable>());
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void run() throws Exception {
        try {
            List<DFSFileState> files = stateManager.fileStateHelper().listFiles(config.basePath);
            if (files != null && !files.isEmpty()) {
                for (DFSFileState fs : files) {
                    long inode = fs.getId();
                    if (inode % config.shardCount == config.shardId) {
                        FileScannerTask task = new FileScannerTask()
                                .fileState(fs)
                                .schemaPath(schemaDir)
                                .fs(this.fs)
                                .hdfsConnection(connection)
                                .stateManager(stateManager);
                        executorService.submit(task);
                        while (executorService.getQueue().size() > MAX_EXECUTOR_QUEUE_SIZE) {
                            Thread.sleep(THREAD_SLEEP_INTERVAL);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            DefaultLogger.LOG.error(ex.getLocalizedMessage());
            throw ex;
        } finally {
            executorService.shutdown();
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    private static class FileScannerTask implements Runnable {
        private DFSFileState fileState;
        private PathInfo schemaPath;
        private FileSystem fs;
        private HdfsConnection hdfsConnection;
        private ZkStateManager stateManager;

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
                CDCDataConverter converter = new CDCDataConverter()
                        .withHdfsConnection(hdfsConnection)
                        .withFileSystem(fs);
                CDCDataConverter.ExtractSchemaResponse response = converter.extractSchema(fileState, schemaPath);
                if (response != null) {
                    fileState.setFileType(response.fileType());
                    fileState.setSchemaLocation(response.schemaPath().pathConfig());
                }
                NameNodeEnv.globalLock().lock();
                try {
                    stateManager.fileStateHelper().update(fileState);
                } finally {
                    NameNodeEnv.globalLock().unlock();
                }
            } catch (Exception ex) {
                DefaultLogger.LOG.error(ex.getLocalizedMessage());
                DefaultLogger.LOG.debug(DefaultLogger.stacktrace(ex));
            }
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class NameNodeFileScannerConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "scanner.files";

        public static class Constants {
            public static final String CONFIG_PATH_FS = "filesystem";
            public static final String CONFIG_FS_TYPE = String.format("%s.type", CONFIG_PATH_FS);
            public static final String CONFIG_HDFS_CONN = "hdfs";
            public static final String CONFIG_SHARD_PATH = "shards";
            public static final String CONFIG_SHARD_COUNT = String.format("%s.count", CONFIG_SHARD_PATH);
            public static final String CONFIG_SHARD_ID = String.format("%s.id", CONFIG_SHARD_PATH);
            public static final String CONFIG_THREAD_COUNT = "threads";
            public static final String CONFIG_SCHEMA_PATH = "schemaDir";
        }

        private String fsType;
        private String hdfsConnection;
        private HierarchicalConfiguration<ImmutableNode> fsConfig;
        private int shardCount = 1;
        private int shardId = 0;
        private Map<String, String> schemaDir;
        private int threads = 1;
        private String basePath;

        public NameNodeFileScannerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            fsConfig = get().configurationAt(Constants.CONFIG_PATH_FS);
            if (fsConfig == null) {
                throw new ConfigurationException(
                        String.format("File Scanner Error: missing configuration. [path=%s]",
                                Constants.CONFIG_PATH_FS));
            }
            fsType = get().getString(Constants.CONFIG_FS_TYPE);
            if (Strings.isNullOrEmpty(fsType)) {
                throw new ConfigurationException(
                        String.format("File Scanner Error: missing configuration. [name=%s]",
                                Constants.CONFIG_FS_TYPE));
            }
            hdfsConnection = get().getString(Constants.CONFIG_HDFS_CONN);
            if (Strings.isNullOrEmpty(hdfsConnection)) {
                throw new ConfigurationException(
                        String.format("File Scanner Error: missing configuration. [name=%s]",
                                Constants.CONFIG_HDFS_CONN));
            }
            schemaDir = ConfigReader.readAsMap(get(), Constants.CONFIG_SCHEMA_PATH);
            if (schemaDir.isEmpty()) {
                throw new ConfigurationException(
                        String.format("File Scanner Error: missing configuration. [name=%s]",
                                Constants.CONFIG_SCHEMA_PATH));
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
        }
    }
}
