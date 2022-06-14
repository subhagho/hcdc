package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.agents.namenode.NameNodeError;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import ai.sapper.hcdc.utils.HadoopDataLoader;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.naming.ConfigurationException;
import java.io.File;

@Getter
@Accessors(fluent = true)
public class NameNodeReplicator {
    private ConnectionManager connectionManager;
    private ZookeeperConnection zkConnection;
    private HdfsConnection hdfsConnection;
    private FileSystem fs;
    private HierarchicalConfiguration<ImmutableNode> config;
    private ReplicatorConfig replicatorConfig;
    private ZkStateManager stateManager;

    @Parameter(names = {"--image", "-i"}, required = true, description = "Path to the FS Image file.")
    private String fsImageFile;
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configfile;
    @Parameter(names = {"--tmp", "-t"}, description = "Temp directory to use to create local files. [DEFAULT=System.getProperty(\"java.io.tmpdir\")]")
    private String tempDir = System.getProperty("java.io.tmpdir");
    
    public void init() throws NameNodeError {
        try {
            config = ConfigReader.read(configfile);
            replicatorConfig = new ReplicatorConfig(config);
            replicatorConfig.read();

            connectionManager = new ConnectionManager();
            connectionManager.init(config, replicatorConfig.connectionPath);

            hdfsConnection = connectionManager.getConnection(replicatorConfig().hdfsConnection(), HdfsConnection.class);
            hdfsConnection.connect();

            zkConnection = connectionManager.getConnection(replicatorConfig().zkConnection(), ZookeeperConnection.class);
            zkConnection.connect();

            fs = hdfsConnection.fileSystem();
            stateManager = new ZkStateManager();
            stateManager.init(config, connectionManager, replicatorConfig.namespace);

        } catch (Throwable t) {
            DefaultLogger.__LOG.error(t.getLocalizedMessage());
            DefaultLogger.__LOG.debug(DefaultLogger.stacktrace(t));
            throw new NameNodeError(t);
        }
    }


    public void run() throws NameNodeError {
        try {

        } catch (Throwable t) {
            DefaultLogger.__LOG.error(t.getLocalizedMessage());
            DefaultLogger.__LOG.debug(DefaultLogger.stacktrace(t));
            throw new NameNodeError(t);
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class ReplicatorConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "replicator";
        private static final String CONFIG_CONNECTIONS = "connections.path";
        private static final String CONFIG_CONNECTION_ZK = "connections.zk";
        private static final String CONFIG_CONNECTION_HDFS = "connections.hdfs";
        private static final String CONFIG_ZK_BASE_PATH = "basePath";
        private static final String CONFIG_NAMESPACE = "namespace";

        private String connectionPath;
        private String zkConnection;
        private String hdfsConnection;
        private String zkBasePath;
        private String namespace;

        public ReplicatorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            connectionPath = get().getString(CONFIG_CONNECTIONS);
            if (Strings.isNullOrEmpty(connectionPath)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_CONNECTIONS));
            }

            zkConnection = get().getString(CONFIG_CONNECTION_ZK);
            if (Strings.isNullOrEmpty(zkConnection)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_CONNECTION_ZK));
            }

            hdfsConnection = get().getString(CONFIG_CONNECTION_HDFS);
            if (Strings.isNullOrEmpty(hdfsConnection)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_CONNECTION_HDFS));
            }

            zkBasePath = get().getString(CONFIG_ZK_BASE_PATH);
            if (Strings.isNullOrEmpty(zkBasePath)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_ZK_BASE_PATH));
            }

            namespace = get().getString(CONFIG_NAMESPACE);
            if (Strings.isNullOrEmpty(namespace)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_NAMESPACE));
            }
        }
    }

    public static void main(String[] args) {
        try {
            NameNodeReplicator replicator = new NameNodeReplicator();
            JCommander.newBuilder().addObject(replicator).build().parse(args);
            replicator.init();
            replicator.run();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
