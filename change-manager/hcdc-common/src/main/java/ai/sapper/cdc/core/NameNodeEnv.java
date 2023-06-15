package ai.sapper.cdc.core;

import ai.sapper.cdc.common.AbstractEnvState;
import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.model.BaseAgentState;
import ai.sapper.cdc.core.model.HCdcProcessingState;
import ai.sapper.cdc.core.model.NameNodeStatus;
import ai.sapper.cdc.core.state.HCdcStateManager;
import ai.sapper.cdc.core.utils.ProtoUtils;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import ai.sapper.cdc.entity.manager.SchemaManager;
import ai.sapper.cdc.entity.manager.SchemaManagerSettings;
import ai.sapper.cdc.entity.model.DbSource;
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
    private String source;
    private NameNodeEnvConfig nEnvConfig;
    private HdfsConnection hdfsConnection;
    private HCdcStateManager stateManager;
    private HCdcSchemaManager schemaManager;
    private HadoopEnvConfig hadoopConfig;
    private NameNodeAdminClient adminClient;
    private DbSource dbSource;

    private final BaseAgentState.AgentState agentState = new BaseAgentState.AgentState();

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
            nEnvConfig = new NameNodeEnvConfig(xmlConfig);
            nEnvConfig.read();

            super.init(xmlConfig, nEnvConfig, new NameNodeEnvState());
            NameNodeEnvSettings settings = (NameNodeEnvSettings) settings();
            source = settings.getSource();
            if (settings.isReadHadoopConfig()) {
                hdfsConnection = connectionManager()
                        .getConnection(settings.getHdfsAdminConnection(),
                                HdfsConnection.class);
                if (hdfsConnection == null) {
                    throw new ConfigurationException("HDFS Admin connection not found.");
                }
                if (!hdfsConnection.isConnected()) hdfsConnection.connect();

                hadoopConfig = new HadoopEnvConfig(settings.getHadoopHome(),
                        settings.getHadoopConfFile(),
                        settings.getHadoopNamespace(),
                        settings.getHadoopInstanceName(),
                        settings.getHadoopVersion())
                        .withNameNodeAdminUrl(settings.getHadoopAdminUrl());
                hadoopConfig.read();
                adminClient = new NameNodeAdminClient(hadoopConfig.nameNodeAdminUrl(), settings.isHadoopUseSSL());
                NameNodeStatus status = adminClient().status();
                if (status != null) {
                    agentState.parseState(status.getState());
                } else {
                    throw new NameNodeError(String.format("Error fetching NameNode status. [url=%s]", adminClient.url()));
                }
            }

            if (super.stateManager() != null) {
                stateManager = (HCdcStateManager) super.stateManager();
                stateManager.checkAgentState(HCdcProcessingState.class);
            }
            HierarchicalConfiguration<ImmutableNode> mConfig
                    = baseConfig().configurationAt(BaseEnvSettings.Constants.__CONFIG_PATH_MANAGERS);
            if (ConfigReader.checkIfNodeExists(mConfig,
                    SchemaManagerSettings.__CONFIG_PATH)) {
                schemaManager = new HCdcSchemaManager();
                schemaManager.init(mConfig,
                        this);
            }

            dbSource = ProtoUtils.build(instance(), moduleInstance().getIp(), "HDFS", 50070);
            state().setState(ENameNodeEnvState.Initialized);
            postInit();

            return this;
        } catch (Throwable t) {
            state().error(t);
            DefaultLogger.stacktrace(t);
            throw new NameNodeError(t);
        }
    }

    public String hdfsTmpDir() {
        Preconditions.checkState(state().isAvailable());
        return ((NameNodeEnvSettings) settings()).getHdfsTmpDir();
    }

    public ENameNodeEnvState error(@NonNull Throwable t) {
        state().error(t);
        return state().getState();
    }

    public synchronized ENameNodeEnvState stop() {
        try {
            if (agentState.getState() == BaseAgentState.EAgentState.Active
                    || agentState.getState() == BaseAgentState.EAgentState.StandBy) {
                agentState.setState(BaseAgentState.EAgentState.Stopped);
            }
            if (state() == null) {
                return null;
            }
            if (state().isAvailable()) {
                try {
                    stateManager.heartbeat(moduleInstance().getInstanceId(), agentState);
                } catch (Exception ex) {
                    LOG.error(ex.getLocalizedMessage());
                    DefaultLogger.stacktrace(ex);
                }
                state().setState(ENameNodeEnvState.Disposed);
            }
            close();

        } catch (Exception ex) {
            LOG.error("Error disposing NameNodeEnv...", ex);
        }
        return state().getState();
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
            DefaultLogger.error(ex.getLocalizedMessage());
            throw new NameNodeError(ex);
        } finally {
            BaseEnv.initUnLock();
        }
    }

    public static ENameNodeEnvState dispose(@NonNull String name) throws NameNodeError {
        BaseEnv.initLock();
        try {
            NameNodeEnv env = BaseEnv.get(name, NameNodeEnv.class);
            if (env == null) {
                throw new NameNodeError(String.format("NameNode Env instance not found. [name=%s]", name));
            }
            if (!BaseEnv.remove(name)) {
                DefaultLogger.warn(String.format("Failed to remove env. [name=%s]", name));
            }
            return env.stop();
        } catch (Exception ex) {
            throw new NameNodeError(ex);
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
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
        }
    }

    public static void audit(@NonNull String name, @NonNull Class<?> caller, @NonNull MessageOrBuilder data) {
        try {
            if (NameNodeEnv.get(name).auditLogger() != null) {
                NameNodeEnv.get(name).auditLogger().audit(caller, System.currentTimeMillis(), data);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
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
                DefaultLogger.stacktrace(ex);
                env.LOG.error(ex.getLocalizedMessage());
            }
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class NameNodeEnvState extends AbstractEnvState<ENameNodeEnvState> {

        public NameNodeEnvState() {
            super(ENameNodeEnvState.Error, ENameNodeEnvState.Unknown);
            setState(ENameNodeEnvState.Unknown);
        }

        public boolean isAvailable() {
            return (getState() == ENameNodeEnvState.Initialized);
        }

        @Override
        public boolean isTerminated() {
            return (getState() == ENameNodeEnvState.Disposed || hasError());
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class NameNodeEnvConfig extends BaseEnvConfig {

        public NameNodeEnvConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, NameNodeEnvSettings.class);
        }

        @Override
        public void read() throws ConfigurationException {
            super.read();
            NameNodeEnvSettings settings = (NameNodeEnvSettings) settings();
            if (settings.isReadHadoopConfig()) {
                if (Strings.isNullOrEmpty(settings.getHadoopHome())) {
                    settings.setHadoopHome(System.getProperty("HADOOP_HOME"));
                    checkStringValue(settings.getHadoopHome(), getClass(), NameNodeEnvSettings.Constants.CONFIG_HADOOP_HOME);
                }
                settings.setHadoopConfig(new File(settings.getHadoopConfFile()));
                if (!settings.getHadoopConfig().exists()) {
                    throw new ConfigurationException(
                            String.format("NameNode Agent Configuration Error: configuration file not found. [%s]",
                                    settings.getHadoopConfig().getAbsolutePath()));
                }
                if (settings.isReadHadoopConfig()) {
                    checkStringValue(settings.getHadoopHome(),
                            getClass(), NameNodeEnvSettings.Constants.CONFIG_HADOOP_HOME);
                    checkStringValue(settings.getHdfsAdminConnection(),
                            getClass(), NameNodeEnvSettings.Constants.CONFIG_CONNECTION_HDFS);
                    checkStringValue(settings.getHadoopNamespace(),
                            getClass(), NameNodeEnvSettings.Constants.CONFIG_HADOOP_NAMESPACE);
                    checkStringValue(settings.getHadoopInstanceName(),
                            getClass(), NameNodeEnvSettings.Constants.CONFIG_HADOOP_INSTANCE);
                    checkStringValue(settings.getHadoopConfFile(),
                            getClass(), NameNodeEnvSettings.Constants.CONFIG_HADOOP_CONFIG);
                }
            }
        }
    }
}
