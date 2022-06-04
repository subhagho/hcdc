package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.common.AbstractState;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.core.connections.ConnectionError;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Accessors(fluent = true)
public class NameNodeEnv {
    public static Logger __LOG = LoggerFactory.getLogger(NameNodeEnv.class);


    private static final String NN_IGNORE_TNX = "%s.IGNORE";

    private final NameNEnvState state = new NameNEnvState();

    private NameNEnvConfig config;
    private HierarchicalConfiguration<ImmutableNode> configNode;
    private ConnectionManager connectionManager;
    private HdfsConnection hdfsConnection;
    private ZkStateManager stateManager;

    public NameNodeEnv init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig, String pathPrefix) throws NameNodeError {
        try {
            if (state.isAvailable()) return this;

            configNode = xmlConfig.configurationAt(pathPrefix);

            this.config = new NameNEnvConfig(xmlConfig, pathPrefix);
            this.config.read();

            connectionManager = new ConnectionManager();
            connectionManager.init(xmlConfig, config.connectionConfigPath);

            hdfsConnection = connectionManager.getConnection(config.hdfsAdminConnection, HdfsConnection.class);
            if (hdfsConnection == null) {
                throw new ConfigurationException("HDFS Admin connection not found.");
            }

            stateManager = new ZkStateManager();
            stateManager.init(configNode, connectionManager);

            state.state(ENameNEnvState.Initialized);
            return this;
        } catch (Throwable t) {
            state.error(t);
            throw new NameNodeError(t);
        }
    }

    public ENameNEnvState stop() {
        if (state.isAvailable()) {
            state.state(ENameNEnvState.Disposed);
        }
        return state.state();
    }

    public String ignoreTnxKey() {
        return String.format(NN_IGNORE_TNX, config.namespace);
    }

    public String namespace() {
        return config.namespace();
    }

    public String hadoopHome() {
        return config.hadoopHome;
    }

    public String hadoopConfigFile() {
        return config.hadoopConfFile;
    }

    public static final NameNodeEnv __instance = new NameNodeEnv();

    public static NameNodeEnv setup(@NonNull HierarchicalConfiguration<ImmutableNode> config, String pathPrefix) throws NameNodeError {
        synchronized (__instance) {
            return __instance.init(config, pathPrefix);
        }
    }

    public static ENameNEnvState dispose() {
        synchronized (__instance) {
            return __instance.stop();
        }
    }

    public static NameNodeEnv get() {
        Preconditions.checkState(__instance.state.isAvailable());
        return __instance;
    }

    public enum ENameNEnvState {
        Unknown, Initialized, Error, Disposed
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class NameNEnvState extends AbstractState<ENameNEnvState> {

        public NameNEnvState() {
            super(ENameNEnvState.Error);
            state(ENameNEnvState.Unknown);
        }

        public boolean isAvailable() {
            return (state() == ENameNEnvState.Initialized);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class NameNEnvConfig extends ConfigReader {
        private static class Constants {
            private static final String CONFIG_NAMESPACE = "namespace";
            private static final String CONFIG_CONNECTIONS = "connections.path";
            private static final String CONFIG_CONNECTION_HDFS = "connections.hdfs-admin";

            private static final String CONFIG_HADOOP_HOME = "hadoop.home";

            private static final String CONFIG_HADOOP_CONFIG = "hadoop.config";
        }

        private String namespace;
        private String connectionConfigPath;
        private String hdfsAdminConnection;
        private String hadoopHome;
        private String hadoopConfFile;

        public NameNEnvConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, path);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not set or is NULL");
            }
            try {
                namespace = get().getString(Constants.CONFIG_NAMESPACE);
                if (Strings.isNullOrEmpty(namespace)) {
                    throw new ConfigurationException(String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_NAMESPACE));
                }
                connectionConfigPath = get().getString(Constants.CONFIG_CONNECTIONS);
                if (Strings.isNullOrEmpty(connectionConfigPath)) {
                    throw new ConfigurationException(String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_CONNECTIONS));
                }
                hdfsAdminConnection = get().getString(Constants.CONFIG_CONNECTION_HDFS);
                if (Strings.isNullOrEmpty(hdfsAdminConnection)) {
                    throw new ConfigurationException(String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION_HDFS));
                }
                hadoopHome = get().getString(Constants.CONFIG_HADOOP_HOME);
                if (Strings.isNullOrEmpty(hadoopHome)) {
                    hadoopHome = System.getProperty("HADOOP_HOME");
                    if (Strings.isNullOrEmpty(hadoopHome))
                        throw new ConfigurationException(String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_HADOOP_HOME));
                }
                hadoopConfFile = get().getString(Constants.CONFIG_HADOOP_CONFIG);
                if (Strings.isNullOrEmpty(hadoopConfFile)) {
                    throw new ConfigurationException(String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_HADOOP_CONFIG));
                }
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }
}
