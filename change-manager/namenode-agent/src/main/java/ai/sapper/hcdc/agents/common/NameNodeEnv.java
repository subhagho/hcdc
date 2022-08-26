package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.agents.namenode.HadoopEnvConfig;
import ai.sapper.hcdc.agents.namenode.NameNodeAdminClient;
import ai.sapper.hcdc.agents.model.NameNodeAgentState;
import ai.sapper.hcdc.agents.model.NameNodeStatus;
import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.NetUtils;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.HdfsConnection;
import ai.sapper.cdc.core.model.ModuleInstance;
import ai.sapper.cdc.core.schema.SchemaManager;
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

import java.net.InetAddress;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class NameNodeEnv extends BaseEnv {
    public static Logger LOG = LoggerFactory.getLogger(NameNodeEnv.class);

    private static final String NN_IGNORE_TNX = "%s.IGNORE";

    private final NameNEnvState state = new NameNEnvState();

    private Thread heartbeatThread;

    private NameNEnvConfig config;
    private HierarchicalConfiguration<ImmutableNode> configNode;
    private HdfsConnection hdfsConnection;
    private ZkStateManager stateManager;
    private SchemaManager schemaManager;
    private List<InetAddress> hostIPs;
    private HadoopEnvConfig hadoopConfig;
    private NameNodeAdminClient adminClient;
    private ModuleInstance moduleInstance;
    private final NameNodeAgentState.AgentState agentState = new NameNodeAgentState.AgentState();

    public synchronized NameNodeEnv init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws NameNodeError {
        try {
            if (state.isAvailable()) return this;

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
                    DefaultLogger.LOG.warn(String.format("Edit Log Processor Shutdown...[state=%s]", state.name()));
                }
            });
            configNode = xmlConfig.configurationAt(NameNEnvConfig.Constants.__CONFIG_PATH);

            this.config = new NameNEnvConfig(xmlConfig);
            this.config.read();

            hostIPs = NetUtils.getInetAddresses();

            super.init(xmlConfig, config.module, config.connectionConfigPath);

            if (config().readHadoopConfig) {
                hdfsConnection = connections().getConnection(config.hdfsAdminConnection, HdfsConnection.class);
                if (hdfsConnection == null) {
                    throw new ConfigurationException("HDFS Admin connection not found.");
                }
                if (!hdfsConnection.isConnected()) hdfsConnection.connect();

                hadoopConfig = new HadoopEnvConfig(config.hadoopHome,
                        config.hadoopConfFile,
                        config.hadoopNamespace,
                        config.hadoopInstanceName,
                        config.hadoopVersion)
                        .withNameNodeAdminUrl(config.hadoopAdminUrl);
                hadoopConfig.read();
                adminClient = new NameNodeAdminClient(hadoopConfig.nameNodeAdminUrl(), config.hadoopUseSSL);
                NameNodeStatus status = adminClient().status();
                if (status != null) {
                    agentState.parseState(status.getState());
                } else {
                    throw new NameNodeError(String.format("Error fetching NameNode status. [url=%s]", adminClient.url()));
                }
            }

            stateManager = config.stateManagerClass.newInstance();
            stateManager.init(configNode, connections(), config.module, config.instance);

            moduleInstance = new ModuleInstance()
                    .withIp(NetUtils.getInetAddress(hostIPs))
                    .withStartTime(System.currentTimeMillis());
            moduleInstance.setSource(config.source);
            moduleInstance.setModule(config.module());
            moduleInstance.setName(config.instance());

            DistributedLock lock = createLock(ZkStateManager.Constants.LOCK_REPLICATION);
            if (lock == null) {
                throw new ConfigurationException(
                        String.format("Replication Lock not defined. [name=%s]",
                                ZkStateManager.Constants.LOCK_REPLICATION));
            }

            stateManager
                    .withReplicationLock(lock)
                    .withModuleInstance(moduleInstance);
            moduleInstance.setInstanceId(moduleInstance.instanceId());
            stateManager.checkAgentState();

            if (ConfigReader.checkIfNodeExists(configNode,
                    SchemaManager.SchemaManagerConfig.Constants.__CONFIG_PATH)) {
                schemaManager = new SchemaManager();
                schemaManager.init(configNode, connections());
            }

            state.state(ENameNEnvState.Initialized);
            if (config.enableHeartbeat) {
                heartbeatThread = new Thread(new HeartbeatThread().withStateManager(stateManager));
                heartbeatThread.start();
            }
            return this;
        } catch (Throwable t) {
            state.error(t);
            throw new NameNodeError(t);
        }
    }


    public ENameNEnvState error(@NonNull Throwable t) {
        state.error(t);
        return state.state();
    }

    public synchronized ENameNEnvState stop() {
        try {
            close();
            if (agentState.state() == NameNodeAgentState.EAgentState.Active
                    || agentState.state() == NameNodeAgentState.EAgentState.StandBy) {
                agentState.state(NameNodeAgentState.EAgentState.Stopped);
            }
            if (state.isAvailable()) {
                try {
                    stateManager.heartbeat(moduleInstance.getInstanceId(), agentState);
                } catch (Exception ex) {
                    DefaultLogger.LOG.error(ex.getLocalizedMessage());
                    DefaultLogger.LOG.debug(DefaultLogger.stacktrace(ex));
                }
                state.state(ENameNEnvState.Disposed);
            }
            if (heartbeatThread != null) {
                heartbeatThread.join();
            }
        } catch (Exception ex) {
            DefaultLogger.LOG.error("Error disposing NameNodeEnv...", ex);
        }
        return state.state();
    }

    public String ignoreTnxKey() {
        return String.format(NN_IGNORE_TNX, config.hadoopNamespace);
    }

    public String module() {
        return config.module();
    }

    public String instance() {
        return config.instance;
    }

    public String source() {
        return config.source;
    }

    public String hadoopHome() {
        return config.hadoopHome;
    }

    public String hadoopConfigFile() {
        return config.hadoopConfFile;
    }

    public DistributedLock createLock(String name) throws NameNodeError {
        if (lockDefs().containsKey(name)) {
            LockDef def = lockDefs().get(name);
            if (def == null) {
                throw new NameNodeError(String.format("No lock definition found: [name=%s]", name));
            }
            return new DistributedLock(def.module(),
                    def.path(),
                    stateManager.basePath())
                    .withConnection(def.connection());
        }
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

    public static ZkStateManager stateManager() {
        return get().stateManager;
    }

    public static ConnectionManager connectionManager() {
        return get().connections();
    }

    public static DistributedLock globalLock() throws NameNodeError {
        return get().createLock(NameNEnvConfig.Constants.LOCK_GLOBAL);
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
            private static final String CONFIG_MODULE = "module";
            private static final String CONFIG_INSTANCE = "instance";
            private static final String CONFIG_HEARTBEAT = "enableHeartbeat";
            private static final String CONFIG_SOURCE_NAME = "source";
            private static final String CONFIG_STATE_MANAGER_TYPE =
                    String.format("%s.stateManagerClass", ZkStateManager.ZkStateManagerConfig.__CONFIG_PATH);
            private static final String CONFIG_CONNECTIONS = "connections.path";
            private static final String CONFIG_CONNECTION_HDFS = "connections.hdfs-admin";

            private static final String CONFIG_HADOOP_HOME = "hadoop.home";
            private static final String CONFIG_HADOOP_INSTANCE = "hadoop.instance";
            private static final String CONFIG_HADOOP_NAMESPACE = "hadoop.namespace";
            private static final String CONFIG_HADOOP_VERSION = "hadoop.version";
            private static final String CONFIG_HADOOP_ADMIN_URL = "hadoop.adminUrl";

            private static final String CONFIG_HADOOP_CONFIG = "hadoop.config";

            private static final String HDFS_NN_USE_HTTPS = "useSSL";
            private static final String LOCK_GLOBAL = "global";
            private static final String CONFIG_LOAD_HADOOP = "needHadoop";
        }

        private String module;
        private String instance;
        private String source;
        private String connectionConfigPath;
        private String hdfsAdminConnection;
        private String hadoopNamespace;
        private String hadoopInstanceName;
        private String hadoopHome;
        private String hadoopAdminUrl;
        private String hadoopConfFile;
        private boolean hadoopUseSSL = true;
        private boolean readHadoopConfig = true;
        private boolean enableHeartbeat = false;
        private short hadoopVersion = 2;

        private Class<? extends ZkStateManager> stateManagerClass = ZkStateManager.class;

        public NameNEnvConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Constants.__CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not set or is NULL");
            }
            try {
                module = get().getString(Constants.CONFIG_MODULE);
                if (Strings.isNullOrEmpty(module)) {
                    throw new ConfigurationException(
                            String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_MODULE));
                }
                instance = get().getString(Constants.CONFIG_INSTANCE);
                if (Strings.isNullOrEmpty(instance)) {
                    throw new ConfigurationException(
                            String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_INSTANCE));
                }
                source = get().getString(Constants.CONFIG_SOURCE_NAME);
                if (Strings.isNullOrEmpty(source)) {
                    throw new ConfigurationException(
                            String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_SOURCE_NAME));
                }
                String ss = get().getString(Constants.CONFIG_LOAD_HADOOP);
                if (!Strings.isNullOrEmpty(ss)) {
                    readHadoopConfig = Boolean.parseBoolean(ss);
                }
                String s = get().getString(Constants.CONFIG_STATE_MANAGER_TYPE);
                if (!Strings.isNullOrEmpty(s)) {
                    stateManagerClass = (Class<? extends ZkStateManager>) Class.forName(s);
                }
                connectionConfigPath = get().getString(Constants.CONFIG_CONNECTIONS);
                if (Strings.isNullOrEmpty(connectionConfigPath)) {
                    throw new ConfigurationException(
                            String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_CONNECTIONS));
                }
                if (get().containsKey(Constants.CONFIG_HEARTBEAT)) {
                    enableHeartbeat = get().getBoolean(Constants.CONFIG_HEARTBEAT);
                }
                if (get().containsKey(Constants.CONFIG_HADOOP_VERSION)) {
                    hadoopVersion = get().getShort(Constants.CONFIG_HADOOP_VERSION);
                }
                if (get().containsKey(Constants.CONFIG_HADOOP_ADMIN_URL)) {
                    hadoopAdminUrl = get().getString(Constants.CONFIG_HADOOP_ADMIN_URL);
                }
                if (readHadoopConfig)
                    readHadoopConfigs();
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }

        private void readHadoopConfigs() throws Exception {
            hadoopNamespace = get().getString(Constants.CONFIG_HADOOP_NAMESPACE);
            if (Strings.isNullOrEmpty(hadoopNamespace)) {
                throw new ConfigurationException(
                        String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_HADOOP_NAMESPACE));
            }
            hadoopInstanceName = get().getString(Constants.CONFIG_HADOOP_INSTANCE);
            if (Strings.isNullOrEmpty(hadoopInstanceName)) {
                throw new ConfigurationException(
                        String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_INSTANCE));
            }

            hdfsAdminConnection = get().getString(Constants.CONFIG_CONNECTION_HDFS);
            if (Strings.isNullOrEmpty(hdfsAdminConnection)) {
                throw new ConfigurationException(
                        String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION_HDFS));
            }
            hadoopHome = get().getString(Constants.CONFIG_HADOOP_HOME);
            if (Strings.isNullOrEmpty(hadoopHome)) {
                hadoopHome = System.getProperty("HADOOP_HOME");
                if (Strings.isNullOrEmpty(hadoopHome))
                    throw new ConfigurationException(
                            String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_HADOOP_HOME));
            }
            hadoopConfFile = get().getString(Constants.CONFIG_HADOOP_CONFIG);
            if (Strings.isNullOrEmpty(hadoopConfFile)) {
                throw new ConfigurationException(
                        String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_HADOOP_CONFIG));
            }
            String s = get().getString(Constants.HDFS_NN_USE_HTTPS);
            if (!Strings.isNullOrEmpty(s)) {
                hadoopUseSSL = Boolean.parseBoolean(s);
            }
        }
    }
}
