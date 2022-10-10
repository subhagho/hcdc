package ai.sapper.cdc.core;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.audit.AuditLogger;
import ai.sapper.cdc.common.utils.NetUtils;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.keystore.KeyStore;
import ai.sapper.cdc.core.model.ModuleInstance;
import ai.sapper.cdc.core.utils.DistributedLockBuilder;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class BaseEnv<T> {
    public static class Constants {
        public static final String CONFIG_ENV = "env";
        public static final String CONFIG_ENV_NAME = String.format("%s.name", CONFIG_ENV);
    }

    private ConnectionManager connectionManager;
    private String storeKey;
    private KeyStore keyStore;
    private final DistributedLockBuilder lockBuilder = new DistributedLockBuilder();
    private String environment;
    private HierarchicalConfiguration<ImmutableNode> rootConfig;
    private List<ExitCallback<T>> exitCallbacks;
    private T state;
    private ModuleInstance moduleInstance;
    private BaseStateManager<?> stateManager;
    private AuditLogger auditLogger;
    private BaseEnvConfig config;
    private List<InetAddress> hostIPs;
    private final String name;
    private Thread heartbeatThread;
    private HeartbeatThread heartbeat;

    public BaseEnv(@NonNull String name) {
        this.name = name;
    }

    public BaseEnv<T> withStoreKey(@NonNull String storeKey) {
        this.storeKey = storeKey;
        return this;
    }

    public BaseEnv<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull T state) throws ConfigurationException {
        try {
            String temp = System.getProperty("java.io.tmpdir");
            temp = String.format("%s/sapper/cdc/%s", temp, getClass().getSimpleName());
            File tdir = new File(temp);
            if (!tdir.exists()) {
                tdir.mkdirs();
            }
            System.setProperty("java.io.tmpdir", temp);
            environment = xmlConfig.getString(Constants.CONFIG_ENV_NAME);
            if (Strings.isNullOrEmpty(environment)) {
                throw new ConfigurationException(
                        String.format("Base Env: missing parameter. [name=%s]", Constants.CONFIG_ENV_NAME));
            }
            config = new BaseEnvConfig(xmlConfig);
            config.read();

            setup(config.module, config.connectionConfigPath);

            hostIPs = NetUtils.getInetAddresses();

            moduleInstance = new ModuleInstance()
                    .withIp(NetUtils.getInetAddress(hostIPs))
                    .withStartTime(System.currentTimeMillis());
            moduleInstance.setSource(config.source);
            moduleInstance.setModule(config.module());
            moduleInstance.setName(config.instance());
            moduleInstance.setInstanceId(moduleInstance.id());

            stateManager = config.stateManagerClass
                    .getDeclaredConstructor().newInstance();
            stateManager.withEnvironment(environment(), name)
                    .withModuleInstance(moduleInstance);
            stateManager
                    .init(config.config(), connectionManager(), config.source);

            rootConfig = config.config();

            if (ConfigReader.checkIfNodeExists(rootConfig,
                    AuditLogger.__CONFIG_PATH)) {
                String c = rootConfig.getString(AuditLogger.CONFIG_AUDIT_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Audit Logger class not specified. [node=%s]",
                                    AuditLogger.CONFIG_AUDIT_CLASS));
                }
                Class<? extends AuditLogger> cls = (Class<? extends AuditLogger>) Class.forName(c);
                AuditLogger auditLogger = cls.getDeclaredConstructor().newInstance();
                auditLogger.init(rootConfig);
            }

            this.state = state;

            if (config.enableHeartbeat) {
                heartbeat = new HeartbeatThread(name()).withStateManager(stateManager);
                heartbeatThread = new Thread(heartbeat);
                heartbeatThread.start();
            }

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void setup(@NonNull String module,
                      @NonNull String connectionsConfigPath) throws ConfigurationException {
        try {

            if (ConfigReader.checkIfNodeExists(rootConfig, KeyStore.__CONFIG_PATH)) {
                String c = rootConfig.getString(KeyStore.CONFIG_KEYSTORE_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Key Store class not defined. [config=%s]", KeyStore.CONFIG_KEYSTORE_CLASS));
                }
                Class<? extends KeyStore> cls = (Class<? extends KeyStore>) Class.forName(c);
                keyStore = cls.getDeclaredConstructor().newInstance();
                keyStore.withPassword(storeKey)
                        .init(rootConfig);
            }
            this.storeKey = null;

            connectionManager = new ConnectionManager()
                    .withKeyStore(keyStore)
                    .withEnv(environment);
            connectionManager.init(rootConfig, connectionsConfigPath);

            lockBuilder.withEnv(environment)
                    .init(rootConfig, module, connectionManager);

        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public synchronized BaseEnv addExitCallback(@NonNull ExitCallback callback) {
        if (exitCallbacks == null) {
            exitCallbacks = new ArrayList<>();
        }
        exitCallbacks.add(callback);
        return this;
    }

    public DistributedLock createLock(@NonNull String path,
                                      @NonNull String module,
                                      @NonNull String name) throws Exception {
        return lockBuilder.createLock(path, module, name);
    }

    public void saveLocks() throws Exception {
        lockBuilder.save();
    }

    public void close() throws Exception {
        if (heartbeatThread != null) {
            heartbeat().terminate();
            heartbeatThread().join();
            heartbeatThread = null;
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
        if (exitCallbacks != null && !exitCallbacks.isEmpty()) {
            for (ExitCallback<T> callback : exitCallbacks) {
                callback.call(state);
            }
        }
    }


    public String module() {
        return moduleInstance.getModule();
    }

    public String instance() {
        return moduleInstance.getName();
    }

    public String source() {
        return moduleInstance.getSource();
    }

    @Getter
    @Accessors(fluent = true)
    public static class BaseEnvConfig extends ConfigReader {
        public static class Constants {
            private static final String CONFIG_MODULE = "module";
            private static final String CONFIG_INSTANCE = "instance";
            private static final String CONFIG_HEARTBEAT = "enableHeartbeat";
            private static final String CONFIG_SOURCE_NAME = "source";
            private static final String CONFIG_STATE_MANAGER_TYPE =
                    String.format("%s.stateManagerClass",
                            BaseStateManager.BaseStateManagerConfig.Constants.__CONFIG_PATH);
            private static final String CONFIG_CONNECTIONS = "connections.path";

        }

        private String module;
        private String instance;
        private String source;
        private String connectionConfigPath;
        private boolean enableHeartbeat = false;
        private Class<? extends BaseStateManager<?>> stateManagerClass;

        public BaseEnvConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, BaseEnv.Constants.CONFIG_ENV);
        }

        public void read() throws Exception {
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
            connectionConfigPath = get().getString(Constants.CONFIG_CONNECTIONS);
            if (Strings.isNullOrEmpty(connectionConfigPath)) {
                throw new ConfigurationException(
                        String.format("NameNode Agent Configuration Error: missing [%s]", Constants.CONFIG_CONNECTIONS));
            }
            if (get().containsKey(Constants.CONFIG_HEARTBEAT)) {
                enableHeartbeat = get().getBoolean(Constants.CONFIG_HEARTBEAT);
            }
            String s = get().getString(Constants.CONFIG_STATE_MANAGER_TYPE);
            if (!Strings.isNullOrEmpty(s)) {
                stateManagerClass = (Class<? extends BaseStateManager<?>>) Class.forName(s);
            }
        }
    }
}
