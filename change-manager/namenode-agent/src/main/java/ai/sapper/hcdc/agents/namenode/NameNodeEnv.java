package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.namenode.model.NameNodeAgentState;
import ai.sapper.hcdc.agents.namenode.model.NameNodeStatus;
import ai.sapper.hcdc.common.AbstractState;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.common.utils.NetUtils;
import ai.sapper.hcdc.core.DistributedLock;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.hdfs.server.namenode.ZkStateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public class NameNodeEnv {
    public static Logger LOG = LoggerFactory.getLogger(NameNodeEnv.class);

    private static final String NN_IGNORE_TNX = "%s.IGNORE";

    private final NameNEnvState state = new NameNEnvState();

    private NameNEnvConfig config;
    private HierarchicalConfiguration<ImmutableNode> configNode;
    private ConnectionManager connectionManager;
    private HdfsConnection hdfsConnection;
    private ZkStateManager stateManager;
    private List<InetAddress> hostIPs;
    private HierarchicalConfiguration<ImmutableNode> hdfsConfig;
    private NameNodeAdminClient adminClient;
    private final Map<String, DistributedLock> locks = new HashMap<>();

    private final NameNodeAgentState.AgentState agentState = new NameNodeAgentState.AgentState();

    public NameNodeEnv init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws NameNodeError {
        try {
            if (state.isAvailable()) return this;

            configNode = xmlConfig.configurationAt(NameNEnvConfig.Constants.__CONFIG_PATH);

            this.config = new NameNEnvConfig(xmlConfig);
            this.config.read();

            hostIPs = NetUtils.getInetAddresses();

            connectionManager = new ConnectionManager();
            connectionManager.init(xmlConfig, config.connectionConfigPath);

            hdfsConnection = connectionManager.getConnection(config.hdfsAdminConnection, HdfsConnection.class);
            if (hdfsConnection == null) {
                throw new ConfigurationException("HDFS Admin connection not found.");
            }
            if (!hdfsConnection.isConnected()) hdfsConnection.connect();

            readHdfsConfig();

            adminClient = new NameNodeAdminClient(config.nameNodeAdminUrl, config.useSSL);

            stateManager = new ZkStateManager();
            stateManager.init(configNode, connectionManager, config.namespace);

            NameNodeStatus status = adminClient().status();
            if (status != null) {
                agentState.parseState(status.getState());
            } else {
                throw new NameNodeError(String.format("Error fetching NameNode status. [url=%s]", adminClient.url()));
            }

            readLocks();

            stateManager.heartbeat(config.nameNodeInstanceName, agentState);

            state.state(ENameNEnvState.Initialized);
            return this;
        } catch (Throwable t) {
            state.error(t);
            throw new NameNodeError(t);
        }
    }

    private void readLocks() throws Exception {
        List<HierarchicalConfiguration<ImmutableNode>> nodes = configNode.configurationsAt(NameNEnvConfig.Constants.CONFIG_LOCK);
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            String name = node.getString(NameNEnvConfig.Constants.CONFIG_LOCK_NAME);
            String conn = node.getString(NameNEnvConfig.Constants.CONFIG_LOCK_CONN);
            String path = name;
            if (node.containsKey(NameNEnvConfig.Constants.CONFIG_LOCK_NODE)) {
                path = node.getString(NameNEnvConfig.Constants.CONFIG_LOCK_NODE);
            }
            ZookeeperConnection connection = connectionManager.getConnection(conn, ZookeeperConnection.class);
            DistributedLock lock = new DistributedLock(config.namespace, path, stateManager.basePath()).withConnection(connection);
            locks.put(name, lock);
        }
    }

    private void readHdfsConfig() throws Exception {
        File cf = new File(config.hadoopConfFile);
        if (!cf.exists()) {
            throw new Exception(String.format("Configuration file not found. ]path=%s]", cf.getAbsolutePath()));
        }
        Configurations configs = new Configurations();
        hdfsConfig = configs.xml(cf);

        List<HierarchicalConfiguration<ImmutableNode>> nodes = hdfsConfig.configurationsAt(NameNEnvConfig.Constants.HDFS_CONFIG_PROPERTY);
        if (nodes == null || nodes.isEmpty()) {
            throw new ConfigurationException(String.format("Failed to read HDFS configuration. [file=%s]", cf.getAbsolutePath()));
        }
        Properties props = new Properties();
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            String sn = node.getString(NameNEnvConfig.Constants.HDFS_CONFIG_PROPERTY_NAME);
            String vn = node.getString(NameNEnvConfig.Constants.HDFS_CONFIG_PROPERTY_VALUE);
            if (!Strings.isNullOrEmpty(sn)) {
                props.put(sn, vn);
            }
        }
        config.nameNodeDataDir = props.getProperty(NameNEnvConfig.Constants.HDFS_NN_DATA_DIR);
        if (Strings.isNullOrEmpty(config.nameNodeDataDir)) {
            throw new Exception(String.format("HDFS Configuration not found. [name=%s][file=%s]",
                    NameNEnvConfig.Constants.HDFS_NN_DATA_DIR, cf.getAbsolutePath()));
        }
        String ns = props.getProperty(NameNEnvConfig.Constants.HDFS_NN_NAMESPACE);
        if (Strings.isNullOrEmpty(ns)) {
            throw new Exception(String.format("HDFS Configuration not found. [name=%s][file=%s]",
                    NameNEnvConfig.Constants.HDFS_NN_NAMESPACE, cf.getAbsolutePath()));
        }
        if (ns.compareToIgnoreCase(config.namespace) != 0) {
            throw new Exception(String.format("HDFS Namespace mismatch. [expected=%s][actual=%s]", ns, config.namespace));
        }
        String nnKey = String.format(NameNEnvConfig.Constants.HDFS_NN_NODES, ns);
        String nns = props.getProperty(nnKey);
        if (Strings.isNullOrEmpty(nns)) {
            throw new Exception(String.format("HDFS Configuration not found. [name=%s][file=%s]", nnKey, cf.getAbsolutePath()));
        }
        String[] parts = nns.split(",");
        String nn = null;
        for (String part : parts) {
            if (part.trim().compareToIgnoreCase(config.nameNodeInstanceName) == 0) {
                nn = part.trim();
                break;
            }
        }
        if (Strings.isNullOrEmpty(nn)) {
            throw new Exception(
                    String.format("NameNode instance not found in HDFS configuration. [instance=%s][namenodes=%s][file=%s]",
                            config.nameNodeInstanceName, nns, cf.getAbsolutePath()));
        }
        LOG.info(String.format("Using NameNode instance [%s.%s]", ns, nn));
        String urlKey = String.format(NameNEnvConfig.Constants.HDFS_NN_URL, ns, nn);
        config.nameNodeAdminUrl = props.getProperty(urlKey);
        if (Strings.isNullOrEmpty(config.nameNodeAdminUrl)) {
            throw new Exception(String.format("NameNode Admin URL not found. [name=%s][file=%s]", urlKey, cf.getAbsolutePath()));
        }
        LOG.info(String.format("Using NameNode Admin UR [%s]", config.nameNodeAdminUrl));
    }

    public ENameNEnvState stop() {
        if (agentState.state() == NameNodeAgentState.EAgentState.Active
                || agentState.state() == NameNodeAgentState.EAgentState.StandBy) {
            agentState.state(NameNodeAgentState.EAgentState.Stopped);
        }
        if (state.isAvailable()) {
            try {
                stateManager.heartbeat(config.nameNodeInstanceName, agentState);
            } catch (Exception ex) {
                DefaultLogger.LOG.error(ex.getLocalizedMessage());
                DefaultLogger.LOG.debug(DefaultLogger.stacktrace(ex));
            }
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

    public DistributedLock lock(String name) {
        if (locks.containsKey(name)) return locks.get(name);
        return null;
    }

    private static final NameNodeEnv __instance = new NameNodeEnv();

    public static NameNodeEnv setup(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws NameNodeError {
        synchronized (__instance) {
            return __instance.init(config);
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

    public static ConnectionManager connectionManager() {
        return get().connectionManager;
    }

    public static ZkStateManager stateManager() {
        return get().stateManager;
    }

    public static DistributedLock globalLock() {
        return get().lock(NameNEnvConfig.Constants.LOCK_GLOBAL);
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
            private static final String __CONFIG_PATH = "agent";
            private static final String CONFIG_NAMESPACE = "namespace";
            private static final String CONFIG_INSTANCE = "instance";
            private static final String CONFIG_CONNECTIONS = "connections.path";
            private static final String CONFIG_CONNECTION_HDFS = "connections.hdfs-admin";

            private static final String CONFIG_HADOOP_HOME = "hadoop.home";

            private static final String CONFIG_HADOOP_CONFIG = "hadoop.config";

            private static final String HDFS_CONFIG_PROPERTY = "property";
            private static final String HDFS_CONFIG_PROPERTY_NAME = "name";
            private static final String HDFS_CONFIG_PROPERTY_VALUE = "value";

            private static final String HDFS_NN_DATA_DIR = "dfs.namenode.name.dir";
            private static final String HDFS_NN_NAMESPACE = "dfs.nameservices";
            private static final String HDFS_NN_NODES = "dfs.ha.namenodes.%s";
            private static final String HDFS_NN_URL = "dfs.namenode.http-address.%s.%s";

            private static final String HDFS_NN_USE_HTTPS = "useSSL";
            private static final String CONFIG_LOCKS = "locks";
            private static final String CONFIG_LOCK = String.format("%s.lock", CONFIG_LOCKS);
            private static final String CONFIG_LOCK_NAME = "name";
            private static final String CONFIG_LOCK_CONN = "connection";
            private static final String CONFIG_LOCK_NODE = "lock-node";
            private static final String LOCK_GLOBAL = "global";
        }

        private String namespace;
        private String nameNodeInstanceName;
        private String connectionConfigPath;
        private String hdfsAdminConnection;
        private String hadoopHome;
        private String hadoopConfFile;
        private String nameNodeDataDir;
        private String nameNodeAdminUrl;
        private boolean useSSL = true;

        public NameNEnvConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Constants.__CONFIG_PATH);
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
                nameNodeInstanceName = get().getString(Constants.CONFIG_INSTANCE);
                if (Strings.isNullOrEmpty(nameNodeInstanceName)) {
                    throw new ConfigurationException(String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_INSTANCE));
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
                String s = get().getString(Constants.HDFS_NN_USE_HTTPS);
                if (!Strings.isNullOrEmpty(s)) {
                    useSSL = Boolean.parseBoolean(s);
                }
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }
}
