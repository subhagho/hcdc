package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.audit.AuditLogger;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.NetUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.BaseStateManager;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.model.CDCAgentState;
import ai.sapper.cdc.core.model.ModuleInstance;
import ai.sapper.cdc.core.schema.SchemaManager;
import ai.sapper.hcdc.agents.model.NameNodeStatus;
import ai.sapper.hcdc.agents.namenode.HadoopEnvConfig;
import ai.sapper.hcdc.agents.namenode.NameNodeAdminClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.MessageOrBuilder;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class NameNodeEnv extends BaseEnv {
    public Logger LOG = LoggerFactory.getLogger(NameNodeEnv.class);

    public static final String NN_IGNORE_TNX = "%s.IGNORE";

    private final NameNEnvState state = new NameNEnvState();
    private final String name;

    private Thread heartbeatThread;
    private AuditLogger auditLogger;
    private NameNEnvConfig config;
    private HierarchicalConfiguration<ImmutableNode> configNode;
    private HdfsConnection hdfsConnection;
    private ZkStateManager stateManager;
    private SchemaManager schemaManager;
    private List<InetAddress> hostIPs;
    private HadoopEnvConfig hadoopConfig;
    private NameNodeAdminClient adminClient;
    private ModuleInstance moduleInstance;
    private final CDCAgentState.AgentState agentState = new CDCAgentState.AgentState();

    public NameNodeEnv(@NonNull String name) {
        this.name = name;
    }

    public synchronized NameNodeEnv init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         @NonNull Class<?> caller) throws NameNodeError {
        try {
            if (state.isAvailable()) return this;

            LOG = LoggerFactory.getLogger(caller);

            Runtime.getRuntime().addShutdownHook(
                    new Thread(new ShutdownThread()
                            .name(name)
                            .env(this)));
            configNode = xmlConfig.configurationAt(NameNEnvConfig.Constants.__CONFIG_PATH);

            this.config = new NameNEnvConfig(xmlConfig);
            this.config.read();

            hostIPs = NetUtils.getInetAddresses();

            super.init(xmlConfig, config.module, config.connectionConfigPath);

            if (config().readHadoopConfig) {
                hdfsConnection = connectionManager().getConnection(config.hdfsAdminConnection, HdfsConnection.class);
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


            moduleInstance = new ModuleInstance()
                    .withIp(NetUtils.getInetAddress(hostIPs))
                    .withStartTime(System.currentTimeMillis());
            moduleInstance.setSource(config.source);
            moduleInstance.setModule(config.module());
            moduleInstance.setName(config.instance());
            moduleInstance.setInstanceId(moduleInstance.id());

            stateManager = config.stateManagerClass.newInstance();
            stateManager.withEnvironment(config.env, name)
                    .withModuleInstance(moduleInstance);
            stateManager
                    .init(configNode, connectionManager(), config.source);

            DistributedLock lock = createLock(BaseStateManager.Constants.LOCK_REPLICATION);
            if (lock == null) {
                throw new ConfigurationException(
                        String.format("Replication Lock not defined. [name=%s]",
                                BaseStateManager.Constants.LOCK_REPLICATION));
            }

            stateManager
                    .withReplicationLock(lock)
                    .withModuleInstance(moduleInstance);
            stateManager.checkAgentState();

            if (ConfigReader.checkIfNodeExists(configNode,
                    SchemaManager.SchemaManagerConfig.Constants.__CONFIG_PATH)) {
                schemaManager = new SchemaManager();
                schemaManager.init(configNode, connectionManager(), config.env, config.source);
            }

            if (ConfigReader.checkIfNodeExists(configNode,
                    AuditLogger.__CONFIG_PATH)) {
                String c = configNode.getString(AuditLogger.CONFIG_AUDIT_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Audit Logger class not specified. [node=%s]",
                                    AuditLogger.CONFIG_AUDIT_CLASS));
                }
                Class<? extends AuditLogger> cls = (Class<? extends AuditLogger>) Class.forName(c);
                auditLogger = cls.newInstance();
                auditLogger.init(configNode);
            }
            state.state(ENameNEnvState.Initialized);
            if (config.enableHeartbeat) {
                heartbeatThread = new Thread(new HeartbeatThread(name).withStateManager(stateManager));
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
            if (agentState.state() == CDCAgentState.EAgentState.Active
                    || agentState.state() == CDCAgentState.EAgentState.StandBy) {
                agentState.state(CDCAgentState.EAgentState.Stopped);
            }
            if (state.isAvailable()) {
                try {
                    stateManager.heartbeat(moduleInstance.getInstanceId(), agentState);
                } catch (Exception ex) {
                    LOG.error(ex.getLocalizedMessage());
                    LOG.debug(DefaultLogger.stacktrace(ex));
                }
                state.state(ENameNEnvState.Disposed);
            }
            close();
            if (heartbeatThread != null) {
                heartbeatThread.join();
            }
        } catch (Exception ex) {
            LOG.error("Error disposing NameNodeEnv...", ex);
        }
        return state.state();
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

    public DistributedLock createLock(@NonNull String name) throws NameNodeError {
        try {
            return createLock(stateManager.zkPath(), name);
        } catch (Exception ex) {
            throw new NameNodeError(ex);
        }
    }

    public DistributedLock globalLock() throws NameNodeError {
        return createLock(NameNEnvConfig.Constants.LOCK_GLOBAL);
    }

    private static final Map<String, NameNodeEnv> __instances = new HashMap<>();

    public static NameNodeEnv setup(@NonNull String name,
                                    @NonNull Class<?> caller,
                                    @NonNull HierarchicalConfiguration<ImmutableNode> config) throws NameNodeError {
        synchronized (__instances) {
            NameNodeEnv env = __instances.get(name);
            if (env == null) {
                env = new NameNodeEnv(name);
                __instances.put(name, env);
            }
            return env.init(config, caller);
        }
    }

    public static ENameNEnvState dispose(@NonNull String name) throws NameNodeError {
        synchronized (__instances) {
            NameNodeEnv env = __instances.remove(name);
            if (env == null) {
                throw new NameNodeError(String.format("NameNode Env instance not found. [name=%s]", name));
            }
            return env.stop();
        }
    }

    public static NameNodeEnv get(@NonNull String name) throws NameNodeError {
        NameNodeEnv env = __instances.get(name);
        if (env == null) {
            throw new NameNodeError(String.format("NameNode Env instance not found. [name=%s]", name));
        }
        Preconditions.checkState(env.state.isAvailable());
        return env;
    }

    public static NameNodeEnv.NameNEnvState status(@NonNull String name) throws NameNodeError {
        NameNodeEnv env = __instances.get(name);
        if (env == null) {
            throw new NameNodeError(String.format("NameNode Env instance not found. [name=%s]", name));
        }
        return env.state;
    }

    public static <T> void audit(@NonNull String name, @NonNull Class<?> caller, @NonNull T data) {
        try {
            if (NameNodeEnv.get(name).auditLogger != null) {
                NameNodeEnv.get(name).auditLogger.audit(caller, System.currentTimeMillis(), data);
            }
        } catch (Exception ex) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
        }
    }

    public static void audit(@NonNull String name, @NonNull Class<?> caller, @NonNull MessageOrBuilder data) {
        try {
            if (NameNodeEnv.get(name).auditLogger != null) {
                NameNodeEnv.get(name).auditLogger.audit(caller, System.currentTimeMillis(), data);
            }
        } catch (Exception ex) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
        }
    }

    public enum ENameNEnvState {
        Unknown, Initialized, Error, Disposed
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    private static class ShutdownThread implements Runnable {
        private String name;
        private NameNodeEnv env;

        @Override
        public void run() {
            try {
                NameNodeEnv.dispose(name);
                env.LOG.warn(String.format("NameNode environment shutdown. [name=%s]", name));
            } catch (Exception ex) {
                env.LOG.debug(DefaultLogger.stacktrace(ex));
                env.LOG.error(ex.getLocalizedMessage());
            }
        }
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
            private static final String CONFIG_ENV = "env";
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

        private String env;
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
                env = get().getString(Constants.CONFIG_ENV);
                if (Strings.isNullOrEmpty(env)) {
                    throw new ConfigurationException(
                            String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_ENV));
                }
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
