package ai.sapper.hcdc.utils;

import ai.sapper.hcdc.agents.namenode.NameNodeEnv;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.naming.ConfigurationException;
import java.io.File;

public class HadoopDataLoader {
    @Parameter(names = {"--config", "-c"}, description = "Configuration File path", required = true)
    private String configfile;
    @Parameter(names = {"--type", "-t"}, description = "File extention to load", required = true)
    private String type;
    @Parameter(names = {"--data", "-d"}, description = "Directory path to load data from.", required = true)
    private String dataFolder;
    private HierarchicalConfiguration<ImmutableNode> config;
    private LoaderConfig loaderConfig;
    private ConnectionManager connectionManager;
    private HdfsConnection connection;
    private FileSystem fs;
    private Path basePath;

    public void run() throws Exception {
        try {
            config = readConfigFile(configfile);
            loaderConfig = new LoaderConfig(config);

            connectionManager = new ConnectionManager();
            connectionManager.init(config, loaderConfig.connectionPath);

            connection = connectionManager.getConnection(loaderConfig.connectionToUse, HdfsConnection.class);
            connection.connect();

            fs = connection.fileSystem();
            basePath = new Path(loaderConfig.baseDir);
            if (!fs.exists(basePath)) {
                fs.mkdirs(basePath);
            }
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(t.getLocalizedMessage());
            DefaultLogger.__LOG.debug(DefaultLogger.stacktrace(t));
            throw new Exception(t);
        }
    }

    private XMLConfiguration readConfigFile(@NonNull String configFile) throws Exception {
        File cf = new File(configFile);
        if (!cf.exists()) {
            throw new Exception(String.format("Configuration file not found. ]path=%s]", cf.getAbsolutePath()));
        }
        Configurations configs = new Configurations();
        return configs.xml(cf);
    }

    @Getter
    @Accessors(fluent = true)
    public static class LoaderConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "loader";
        private static final String CONFIG_CONNECTIONS = "connections.path";
        private static final String CONFIG_CONNECTION_HDFS = "connections.use";
        private static final String CONFIG_BATCH_SIZE = "batchSize";
        private static final String CONFIG_BASE_DIR = "baseDir";

        private String connectionPath;
        private long batchSize = 1024 * 1024 * 15;
        private String baseDir;
        private String connectionToUse;

        public LoaderConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            connectionPath = get().getString(CONFIG_CONNECTIONS);
            if (Strings.isNullOrEmpty(connectionPath)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_CONNECTIONS));
            }
            baseDir = get().getString(CONFIG_BASE_DIR);
            if (Strings.isNullOrEmpty(baseDir)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_BASE_DIR));
            }
            connectionToUse = get().getString(CONFIG_CONNECTION_HDFS);
            if (Strings.isNullOrEmpty(connectionToUse)) {
                throw new ConfigurationException(String.format("HDFS Data Loader Configuration Error: missing [%s]", CONFIG_CONNECTION_HDFS));
            }
            String s = get().getString(CONFIG_BATCH_SIZE);
            if (!Strings.isNullOrEmpty(s)) {
                batchSize = Long.parseLong(s);
            }
        }
    }

    public static void main(String[] args) {
        try {
            HadoopDataLoader loader = new HadoopDataLoader();
            JCommander.newBuilder().addObject(loader).build().parse(args);
            loader.run();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
