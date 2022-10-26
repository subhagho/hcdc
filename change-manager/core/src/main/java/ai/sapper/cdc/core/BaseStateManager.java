package ai.sapper.cdc.core;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Getter
@Accessors(fluent = true)
public abstract class BaseStateManager<T> implements Closeable {
    public static class Constants {
        public static final int LOCK_RETRY_COUNT = 8;
        public static final String ZK_PATH_HEARTBEAT = "/heartbeat";
        public static final String ZK_PATH_PROCESS_STATE = "state";

        public static final String LOCK_REPLICATION = "LOCK_REPLICATION";
    }

    private ZookeeperConnection connection;
    private BaseStateManagerConfig config;
    private String zkPath;
    private String zkAgentStatePath;
    private String zkModulePath;
    private String zkAgentPath;

    private ProcessingState<T> processingState;
    private ModuleInstance moduleInstance;
    private String name;
    private String environment;
    @Getter(AccessLevel.NONE)
    private DistributedLock replicationLock;
    private Class<? extends ProcessingState<T>> type;

    public void stateLock() throws Exception {
        int retryCount = 0;
        while (true) {
            try {
                replicationLock.lock();
                break;
            } catch (DistributedLock.LockError le) {
                if (retryCount > Constants.LOCK_RETRY_COUNT) {
                    throw new Exception(
                            String.format("Error acquiring lock. [error=%s][retries=%d]",
                                    le.getLocalizedMessage(), retryCount));
                }
                DefaultLogger.LOGGER.warn(String.format("Failed to acquire lock, will retry... [error=%s][retries=%d]",
                        le.getLocalizedMessage(), retryCount));
                Thread.sleep(500);
                retryCount++;
            }
        }
    }

    public void stateUnlock() {
        replicationLock.unlock();
    }

    public BaseStateManager<T> withEnvironment(@NonNull String environment,
                                               @NonNull String name) {
        this.environment = environment;
        this.name = name;

        return this;
    }


    public BaseStateManager<T> withModuleInstance(@NonNull ModuleInstance moduleInstance) {
        this.moduleInstance = moduleInstance;
        return this;
    }

    public BaseStateManager<T> withConfig(@NonNull BaseStateManagerConfig config) {
        this.config = config;
        return this;
    }

    public String basePath() {
        return config().basePath();
    }

    public BaseStateManager<T> init(@NonNull BaseEnv<?> env) throws ManagerStateError {
        try {
            Preconditions.checkNotNull(moduleInstance);
            Preconditions.checkNotNull(config);
            Preconditions.checkState(!Strings.isNullOrEmpty(environment));

            connection = env.connectionManager()
                    .getConnection(config.zkConnection(),
                            ZookeeperConnection.class);
            Preconditions.checkNotNull(connection);
            if (!connection.isConnected()) connection.connect();
            CuratorFramework client = connection().client();
            zkPath = new PathUtils.ZkPathBuilder(basePath())
                    .withPath(environment)
                    .build();
            zkModulePath = new PathUtils.ZkPathBuilder(zkPath)
                    .withPath(moduleInstance.getModule())
                    .build();
            zkAgentPath = new PathUtils.ZkPathBuilder(zkModulePath)
                    .withPath(moduleInstance.getName())
                    .build();

            replicationLock = env.createLock(zkPath,
                    moduleInstance.getModule(),
                    Constants.LOCK_REPLICATION);
            if (replicationLock == null) {
                throw new ConfigurationException(
                        String.format("Replication Lock not defined. [name=%s]",
                                Constants.LOCK_REPLICATION));
            }

            if (client.checkExists().forPath(zkAgentPath) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkAgentPath);
                if (Strings.isNullOrEmpty(path)) {
                    throw new ManagerStateError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
            }
            zkAgentStatePath = new PathUtils.ZkPathBuilder(zkAgentPath)
                    .withPath(Constants.ZK_PATH_PROCESS_STATE)
                    .build();
            return this;
        } catch (Exception ex) {
            throw new ManagerStateError(ex);
        }
    }

    public abstract BaseStateManager<T> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                             @NonNull BaseEnv<?> env,
                                             @NonNull String source) throws StateManagerError;

    public synchronized void checkState() {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
    }

    public void checkAgentState(@NonNull Class<? extends ProcessingState<T>> type) throws Exception {
        stateLock();
        try {
            CuratorFramework client = connection().client();

            if (client.checkExists().forPath(zkAgentStatePath) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkAgentStatePath);
                if (Strings.isNullOrEmpty(path)) {
                    throw new ManagerStateError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
                processingState = type.getDeclaredConstructor().newInstance();
                processingState.setNamespace(moduleInstance.getModule());
                processingState.setUpdatedTime(0);

                String json = JSONUtils.asString(processingState, LongTxState.class);
                client.setData().forPath(zkAgentStatePath, json.getBytes(StandardCharsets.UTF_8));
            } else {
                processingState = readState(type);
            }
            processingState.setInstance(moduleInstance);
            update(processingState);

            this.type = type;
        } finally {
            stateUnlock();
        }
    }

    public ProcessingState<T> initState(T txId) throws ManagerStateError {
        checkState();
        Preconditions.checkNotNull(type);
        if (processingState.compareTx(txId) >= 0) return processingState;

        try {
            stateLock();
            try {
                processingState = readState(type);
                processingState.setProcessedTxId(txId);
                processingState.setUpdatedTime(System.currentTimeMillis());

                return update(processingState);
            } finally {
                stateUnlock();
            }
        } catch (Exception ex) {
            throw new ManagerStateError(ex);
        }
    }

    public ProcessingState<T> update(ProcessingState<T> state) throws Exception {
        state.setUpdatedTime(System.currentTimeMillis());

        CuratorFramework client = connection().client();
        String json = JSONUtils.asString(state, state.getClass());
        client.setData().forPath(zkAgentStatePath, json.getBytes(StandardCharsets.UTF_8));

        return state;
    }

    public ProcessingState<T> update(T processedTxId) throws ManagerStateError {
        checkState();
        Preconditions.checkNotNull(type);
        if (processingState.compareTx(processedTxId) >= 0) return processingState;

        try {
            stateLock();
            try {
                processingState = readState(type);
                processingState.setProcessedTxId(processedTxId);
                return update(processingState);
            } finally {
                stateUnlock();
            }
        } catch (Exception ex) {
            throw new ManagerStateError(ex);
        }
    }

    public ProcessingState<T> updateMessageId(String messageId) throws ManagerStateError {
        checkState();
        Preconditions.checkNotNull(type);
        try {
            stateLock();
            try {
                processingState = readState(type);
                processingState.setCurrentMessageId(messageId);
                return update(processingState);
            } finally {
                stateUnlock();
            }
        } catch (Exception ex) {
            throw new ManagerStateError(ex);
        }
    }

    private ProcessingState<T> readState(Class<? extends ProcessingState<T>> type) throws ManagerStateError {
        try {
            CuratorFramework client = connection().client();
            byte[] data = client.getData().forPath(zkAgentStatePath);
            if (data != null && data.length > 0) {
                String json = new String(data, StandardCharsets.UTF_8);
                return JSONUtils.read(json, type);
            }
            throw new ManagerStateError(String.format("NameNode State not found. [path=%s]", zkPath));
        } catch (Exception ex) {
            throw new ManagerStateError(ex);
        }
    }

    @Override
    public void close() throws IOException {
        if (replicationLock != null) {
            replicationLock.close();
        }
    }

    public abstract Heartbeat heartbeat(@NonNull String instance) throws ManagerStateError;

    public Heartbeat heartbeat(@NonNull String name, @NonNull CDCAgentState.AgentState state) throws ManagerStateError {
        checkState();
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                String path = getHeartbeatPath(name);
                if (client.checkExists().forPath(path) == null) {
                    path = client.create().creatingParentContainersIfNeeded().forPath(path);
                    if (Strings.isNullOrEmpty(path)) {
                        throw new ManagerStateError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                    }
                }
                Heartbeat heartbeat = new Heartbeat();
                heartbeat.setName(name);
                heartbeat.setType(state.getClass().getCanonicalName());
                heartbeat.setState(state.state().name());
                if (state.hasError()) {
                    heartbeat.setError(state.error());
                }
                heartbeat.setTimestamp(System.currentTimeMillis());
                heartbeat.setModule(moduleInstance);
                String json = JSONUtils.asString(heartbeat, Heartbeat.class);
                client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

                return heartbeat;
            } catch (Exception ex) {
                throw new ManagerStateError(ex);
            }
        }
    }

    public Heartbeat heartbeat(@NonNull String name, @NonNull String state) throws ManagerStateError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        try {
            CuratorFramework client = connection().client();
            String path = getHeartbeatPath(name);
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    String json = new String(data, StandardCharsets.UTF_8);
                    return JSONUtils.read(json, Heartbeat.class);
                } else {
                    Heartbeat hb = new Heartbeat();
                    hb.setName(name);
                    hb.setModule(moduleInstance);
                    hb.setState(state);

                    String json = JSONUtils.asString(hb, Heartbeat.class);
                    client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

                    return hb;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new ManagerStateError(ex);
        }
    }


    private String getHeartbeatPath(String name) {
        return new PathUtils.ZkPathBuilder(zkModulePath)
                .withPath(Constants.ZK_PATH_HEARTBEAT)
                .withPath(name)
                .build();
    }

    @Getter
    @Accessors(fluent = true)
    public static abstract class BaseStateManagerConfig extends ConfigReader {

        public static final class Constants {
            public static final String __CONFIG_PATH = "state";
            public static final String CONFIG_ZK_BASE = "basePath";
            public static final String CONFIG_ZK_CONNECTION = "connection";
        }

        private String basePath;
        private String zkConnection;

        public BaseStateManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                      @NonNull String path) {
            super(config, path);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Domain Manager Configuration not set or is NULL");
            }
            try {
                basePath = get().getString(Constants.CONFIG_ZK_BASE);
                if (Strings.isNullOrEmpty(basePath)) {
                    throw new ConfigurationException(String.format("Domain Manager Configuration Error: missing [%s]",
                            Constants.CONFIG_ZK_BASE));
                }
                basePath = basePath.trim();
                if (basePath.endsWith("/")) {
                    basePath = basePath.substring(0, basePath.length() - 2);
                }
                zkConnection = get().getString(Constants.CONFIG_ZK_CONNECTION);
                if (Strings.isNullOrEmpty(zkConnection)) {
                    throw new ConfigurationException(String.format("Domain Manager Configuration Error: missing [%s]",
                            Constants.CONFIG_ZK_CONNECTION));
                }
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing Domain Manager configuration.", t);
            }
        }
    }
}
