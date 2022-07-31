package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import ai.sapper.hcdc.core.io.FileSystem;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

public class NameNodeFileScanner {
    private FileSystem fs;
    private NameNodeFileScannerConfig config;
    private HdfsConnection connection;
    private FileSystem.FileSystemMocker fileSystemMocker;
    private ZkStateManager stateManager;

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

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void run() throws Exception {
        try {

        } catch (Exception ex) {
            DefaultLogger.LOG.error(ex.getLocalizedMessage());
            throw ex;
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
            public static final String CONFIG_SHARD_COUNT = "shards.count";
            public static final String CONFIG_SHARD_ID = "shards.id";
            public static final String CONFIG_THREAD_COUNT = "threads";
            public static final String CONFIG_SCHEMA_PATH = "schemaDir";
        }

        private String fsType;
        private String hdfsConnection;
        private HierarchicalConfiguration<ImmutableNode> fsConfig;
        private int shardCount;
        private int shardId;
        private String schemaDir;

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
            schemaDir = get().getString(Constants.CONFIG_SCHEMA_PATH);
            if (Strings.isNullOrEmpty(schemaDir)) {
                throw new ConfigurationException(
                        String.format("File Scanner Error: missing configuration. [name=%s]",
                                Constants.CONFIG_SCHEMA_PATH));
            }
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
    }
}
