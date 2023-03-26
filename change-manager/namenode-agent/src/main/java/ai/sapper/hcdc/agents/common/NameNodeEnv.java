package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.AbstractEnvState;
import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.ExitCallback;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.model.CDCAgentState;
import ai.sapper.cdc.core.model.LongTxState;
import ai.sapper.cdc.entity.model.DbSource;
import ai.sapper.cdc.entity.schema.SchemaManager;
import ai.sapper.hcdc.agents.model.NameNodeStatus;
import ai.sapper.hcdc.agents.namenode.HadoopEnvConfig;
import ai.sapper.hcdc.agents.namenode.NameNodeAdminClient;
import ai.sapper.cdc.core.utils.ProtoUtils;
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

@Getter
@Accessors(fluent = true)
public class NameNodeEnv extends BaseEnv<NameNodeEnv.ENameNodeEnvState> {
    public final Logger LOG;

    public static final String NN_IGNORE_TNX = "%s.IGNORE";

    private NameNodeEnvConfig nEnvConfig;
    private HierarchicalConfiguration<ImmutableNode> configNode;
    private HdfsConnection hdfsConnection;
    private ZkStateManager stateManager;
    private SchemaManager schemaManager;
    private HadoopEnvConfig hadoopConfig;
    private NameNodeAdminClient adminClient;
    private DbSource dbSource;

    private final CDCAgentState.AgentState agentState = new CDCAgentState.AgentState();

    public NameNodeEnv(@NonNull String name, @NonNull Class<?> caller) {
        super(name);
        LOG = LoggerFactory.getLogger(caller);
    }

    public synchronized NameNodeEnv init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         @NonNull Class<?> caller) throws NameNodeError {
        try {
            if (state() != null && state().isAvailable()) return this;

            Runtime.getRuntime().addShutdownHook(
                    new Thread(new ShutdownThread()
                            .name(name())
                            .env(this)));
            super.init(xmlConfig, new NameNodeEnvState());

            configNode = rootConfig().configurationAt(NameNodeEnvConfig.Constants.__CONFIG_PATH);

            this.nEnvConfig = new NameNodeEnvConfig(rootConfig());
            this.nEnvConfig.read();

            if (nEnvConfig().readHadoopConfig) {
                hdfsConnection = connectionManager()
                        .getConnection(nEnvConfig.hdfsAdminConnection,
                                HdfsConnection.class);
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

            dbSource = ProtoUtils.build(instance(), moduleInstance().getIp(), "HADOOP", 50070);
            state().state(ENameNodeEnvState.Initialized);
            postInit();

            return this;
        } catch (Throwable t) {
            if (state() != null)
                state().error(t);
            throw new NameNodeError(t);
        }
    }

    public String hdfsTmpDir() {
        return nEnvConfig().hdfsTmpDir();
    }

    public ENameNodeEnvState error(@NonNull Throwable t) {
        state().error(t);
        return state().state();
    }

    public synchronized ENameNodeEnvState stop() {
        try {
            if (agentState.state() == CDCAgentState.EAgentState.Active
                    || agentState.state() == CDCAgentState.EAgentState.StandBy) {
                agentState.state(CDCAgentState.EAgentState.Stopped);
            }
            if (state().isAvailable()) {
                try {
                    stateManager.heartbeat(moduleInstance().getInstanceId(), agentState);
                } catch (Exception ex) {
                    LOG.error(ex.getLocalizedMessage());
                    LOG.debug(DefaultLogger.stacktrace(ex));
                }
                state().state(ENameNodeEnvState.Disposed);
            }
            close();

        } catch (Exception ex) {
            LOG.error("Error disposing NameNodeEnv...", ex);
        }
        return state().state();
    }


    public static NameNodeEnv setup(@NonNull String name,
                                    @NonNull Class<?> caller,
                                    @NonNull HierarchicalConfiguration<ImmutableNode> config) throws NameNodeError {
        BaseEnv.initLock();
        try {
            NameNodeEnv env = BaseEnv.get(name, NameNodeEnv.class);
            if (env == null) {
                env = new NameNodeEnv(name, caller);
                BaseEnv.add(name, env);
            }
            return env.init(config, caller);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            throw new NameNodeError(ex);
        } finally {
            BaseEnv.initUnLock();
        }
    }

    public static ENameNodeEnvState dispose(@NonNull String name) throws NameNodeError {
        BaseEnv.initLock();
        try {
            NameNodeEnv env = (NameNodeEnv) BaseEnv.remove(name);
            if (env == null) {
                throw new NameNodeError(String.format("NameNode Env instance not found. [name=%s]", name));
            }
            return env.stop();
        } finally {
            BaseEnv.initUnLock();
        }
    }

    public static NameNodeEnv get(@NonNull String name) throws NameNodeError {
        try {
            NameNodeEnv env = BaseEnv.get(name, NameNodeEnv.class);
            if (env == null) {
                throw new NameNodeError(String.format("NameNode Env instance not found. [name=%s]", name));
            }
            Preconditions.checkState(env.state().isAvailable());
            return env;
        } catch (Exception ex) {
            throw new NameNodeError(ex);
        }
    }

    public static NameNodeEnvState status(@NonNull String name) throws NameNodeError {
        try {
            NameNodeEnv env = BaseEnv.get(name, NameNodeEnv.class);
            if (env == null) {
                throw new NameNodeError(String.format("NameNode Env instance not found. [name=%s]", name));
            }
            return (NameNodeEnvState) env.state();
        } catch (Exception ex) {
            throw new NameNodeError(ex);
        }
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

    @SuppressWarnings("unchecked")
    public <S extends SchemaManager> S schemaManager(@NonNull Class<? extends SchemaManager> type) throws Exception {
        if (schemaManager != null && !ReflectionUtils.isSuperType(type, schemaManager.getClass())) {
            throw new Exception(
                    String.format("Invalid SchemaManager type. [expected=%s][actual=%s]",
                            type.getCanonicalName(), schemaManager.getClass().getCanonicalName()));
        }
        return (S) schemaManager;
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
                    for (ExitCallback<ENameNodeEnvState> callback : env.exitCallbacks()) {
                        callback.call(env.state());
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
    public static class NameNodeEnvState extends AbstractEnvState<ENameNodeEnvState> {

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
            private static final String CONFIG_HADOOP_TMP_DIR = "hadoop.tmpDir";

            private static final String CONFIG_HADOOP_CONFIG = "hadoop.config";

            private static final String HDFS_NN_USE_HTTPS = "useSSL";
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
        private String hdfsTmpDir = "/tmp/";

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
                if (get().containsKey(Constants.CONFIG_HADOOP_TMP_DIR)) {
                    hdfsTmpDir = get().getString(Constants.CONFIG_HADOOP_TMP_DIR);
                }
                if (readHadoopConfig)
                    readHadoopConfigs();
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }

        private void readHadoopConfigs() throws Exception {
            hadoopNamespace = get().getString(Constants.CONFIG_HADOOP_NAMESPACE);
            checkStringValue(hadoopNamespace, getClass(), Constants.CONFIG_HADOOP_NAMESPACE);
            hadoopInstanceName = get().getString(Constants.CONFIG_HADOOP_INSTANCE);
            checkStringValue(hadoopInstanceName, getClass(), Constants.CONFIG_HADOOP_INSTANCE);
            hdfsAdminConnection = get().getString(Constants.CONFIG_CONNECTION_HDFS);
            checkStringValue(hdfsAdminConnection, getClass(), Constants.CONFIG_CONNECTION_HDFS);
            hadoopHome = get().getString(Constants.CONFIG_HADOOP_HOME);
            if (Strings.isNullOrEmpty(hadoopHome)) {
                hadoopHome = System.getProperty("HADOOP_HOME");
                if (Strings.isNullOrEmpty(hadoopHome))
                    checkStringValue(hadoopHome, getClass(), Constants.CONFIG_HADOOP_HOME);
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
