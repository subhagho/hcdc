package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.*;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.model.CDCAgentState;
import ai.sapper.cdc.core.model.LongTxState;
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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class NameNodeEnv extends BaseEnv<NameNodeEnv.NameNodeEnvState> {
    public final Logger LOG;

    public static final String NN_IGNORE_TNX = "%s.IGNORE";

    private final NameNodeEnvState state = new NameNodeEnvState();

    private NameNodeEnvConfig nEnvConfig;
    private HierarchicalConfiguration<ImmutableNode> configNode;
    private HdfsConnection hdfsConnection;
    private ZkStateManager stateManager;
    private SchemaManager schemaManager;
    private HadoopEnvConfig hadoopConfig;
    private NameNodeAdminClient adminClient;

    private final CDCAgentState.AgentState agentState = new CDCAgentState.AgentState();

    public NameNodeEnv(@NonNull String name, @NonNull Class<?> caller) {
        super(name);
        LOG = LoggerFactory.getLogger(caller);
    }

    public synchronized NameNodeEnv init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         @NonNull Class<?> caller) throws NameNodeError {
        try {
            if (state.isAvailable()) return this;

            Runtime.getRuntime().addShutdownHook(
                    new Thread(new ShutdownThread()
                            .name(name())
                            .env(this)));
            super.init(xmlConfig, state);

            configNode = rootConfig().configurationAt(NameNodeEnvConfig.Constants.__CONFIG_PATH);

            this.nEnvConfig = new NameNodeEnvConfig(rootConfig());
            this.nEnvConfig.read();

            if (nEnvConfig().readHadoopConfig) {
                hdfsConnection = connectionManager().getConnection(nEnvConfig.hdfsAdminConnection, HdfsConnection.class);
                if (hdfsConnection == null) {
                    throw new ConfigurationException("HDFS Admin connection not found.");
                }
                if (!hdfsConnection.isConnected()) hdfsConnection.connect();

                hadoopConfig = new HadoopEnvConfig(nEnvConfig.hadoopHome,
                        nEnvConfig.hadoopConfFile,
                        nEnvConfig.hadoopNamespace,
                        nEnvConfig.hadoopInstanceName,
                        nEnvConfig.hadoopVersion)
                        .withNameNodeAdminUrl(nEnvConfig.hadoopAdminUrl);
                hadoopConfig.read();
                adminClient = new NameNodeAdminClient(hadoopConfig.nameNodeAdminUrl(), nEnvConfig.hadoopUseSSL);
                NameNodeStatus status = adminClient().status();
                if (status != null) {
                    agentState.parseState(status.getState());
                } else {
                    throw new NameNodeError(String.format("Error fetching NameNode status. [url=%s]", adminClient.url()));
                }
            }

            if (super.stateManager() != null) {
                stateManager = (ZkStateManager) super.stateManager();

                DistributedLock lock = createLock(BaseStateManager.Constants.LOCK_REPLICATION);
                if (lock == null) {
                    throw new ConfigurationException(
                            String.format("Replication Lock not defined. [name=%s]",
                                    BaseStateManager.Constants.LOCK_REPLICATION));
                }

                stateManager
                        .withReplicationLock(lock);
                stateManager.checkAgentState(LongTxState.class);
            }
            if (ConfigReader.checkIfNodeExists(config().config(),
                    SchemaManager.SchemaManagerConfig.Constants.__CONFIG_PATH)) {
                schemaManager = new SchemaManager();
                schemaManager.init(config().config(),
                        connectionManager(),
                        environment(),
                        source(),
                        dLockBuilder().zkPath());
            }

            state.state(ENameNodeEnvState.Initialized);

            return this;
        } catch (Throwable t) {
            state.error(t);
            throw new NameNodeError(t);
        }
    }


    public ENameNodeEnvState error(@NonNull Throwable t) {
        state.error(t);
        return state.state();
    }

    public synchronized ENameNodeEnvState stop() {
        try {
            if (agentState.state() == CDCAgentState.EAgentState.Active
                    || agentState.state() == CDCAgentState.EAgentState.StandBy) {
                agentState.state(CDCAgentState.EAgentState.Stopped);
            }
            if (state.isAvailable()) {
                try {
                    stateManager.heartbeat(moduleInstance().getInstanceId(), agentState);
                } catch (Exception ex) {
                    LOG.error(ex.getLocalizedMessage());
                    LOG.debug(DefaultLogger.stacktrace(ex));
                }
                state.state(ENameNodeEnvState.Disposed);
            }
            close();

        } catch (Exception ex) {
            LOG.error("Error disposing NameNodeEnv...", ex);
        }
        return state.state();
    }

    public DistributedLock createLock(@NonNull String name) throws NameNodeError {
        try {
            return createLock(stateManager.zkPath(), module(), name);
        } catch (Exception ex) {
            throw new NameNodeError(ex);
        }
    }

    public DistributedLock globalLock() throws NameNodeError {
        return createLock(NameNodeEnvConfig.Constants.LOCK_GLOBAL);
    }

    private static final Map<String, NameNodeEnv> __instances = new HashMap<>();

    public static NameNodeEnv setup(@NonNull String name,
                                    @NonNull Class<?> caller,
                                    @NonNull HierarchicalConfiguration<ImmutableNode> config) throws NameNodeError {
        synchronized (__instances) {
            NameNodeEnv env = __instances.get(name);
            if (env == null) {
                env = new NameNodeEnv(name, caller);
                __instances.put(name, env);
            }
            return env.init(config, caller);
        }
    }

    public static ENameNodeEnvState dispose(@NonNull String name) throws NameNodeError {
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

    public static NameNodeEnvState status(@NonNull String name) throws NameNodeError {
        NameNodeEnv env = __instances.get(name);
        if (env == null) {
            throw new NameNodeError(String.format("NameNode Env instance not found. [name=%s]", name));
        }
        return env.state;
    }

    public static <T> void audit(@NonNull String name, @NonNull Class<?> caller, @NonNull T data) {
        try {
            if (NameNodeEnv.get(name).auditLogger() != null) {
                NameNodeEnv.get(name).auditLogger().audit(caller, System.currentTimeMillis(), data);
            }
        } catch (Exception ex) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
        }
    }

    public static void audit(@NonNull String name, @NonNull Class<?> caller, @NonNull MessageOrBuilder data) {
        try {
            if (NameNodeEnv.get(name).auditLogger() != null) {
                NameNodeEnv.get(name).auditLogger().audit(caller, System.currentTimeMillis(), data);
            }
        } catch (Exception ex) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
        }
    }

    public enum ENameNodeEnvState {
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
                if (env.exitCallbacks() != null) {
                    for (ExitCallback callback : env.exitCallbacks()) {
                        callback.call(env.state);
                    }
                }
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
    public static class NameNodeEnvState extends AbstractState<ENameNodeEnvState> {

        public NameNodeEnvState() {
            super(ENameNodeEnvState.Error);
            state(ENameNodeEnvState.Unknown);
        }

        public boolean isAvailable() {
            return (state() == ENameNodeEnvState.Initialized);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class NameNodeEnvConfig extends ConfigReader {
        private static class Constants {
            private static final String __CONFIG_PATH = "agent";

            private static final String CONFIG_CONNECTION_HDFS = "hadoop.hdfs-admin";

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

        private String hdfsAdminConnection;
        private String hadoopNamespace;
        private String hadoopInstanceName;
        private String hadoopHome;
        private String hadoopAdminUrl;
        private String hadoopConfFile;
        private File hadoopConfig;
        private boolean hadoopUseSSL = true;
        private boolean readHadoopConfig = true;
        private short hadoopVersion = 2;

        public NameNodeEnvConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Constants.__CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not set or is NULL");
            }
            try {

                String ss = get().getString(Constants.CONFIG_LOAD_HADOOP);
                if (!Strings.isNullOrEmpty(ss)) {
                    readHadoopConfig = Boolean.parseBoolean(ss);
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
                        String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_HADOOP_INSTANCE));
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
            hadoopConfig = ConfigReader.readFileNode(get(), Constants.CONFIG_HADOOP_CONFIG);
            if (hadoopConfig == null || !hadoopConfig.exists()) {
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
