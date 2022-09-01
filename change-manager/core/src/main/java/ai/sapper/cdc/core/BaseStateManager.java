package ai.sapper.cdc.core;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.AgentTxState;
import ai.sapper.cdc.core.model.CDCAgentState;
import ai.sapper.cdc.core.model.Heartbeat;
import ai.sapper.cdc.core.model.ModuleInstance;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;

@Getter
@Accessors(fluent = true)
public abstract class BaseStateManager {
    public static class Constants {
        public static final String ZK_PATH_HEARTBEAT = "/heartbeat";
        public static final String ZK_PATH_PROCESS_STATE = "state";

        public static final String LOCK_REPLICATION = "LOCK_REPLICATION";
    }

    private ZookeeperConnection connection;
    private BaseStateManagerConfig config;
    private String zkPath;
    private String zkAgentStatePath;
    private AgentTxState agentTxState;
    private ModuleInstance moduleInstance;
    private String name;
    private String environment;

    public BaseStateManager withEnvironment(@NonNull String environment,
                                            @NonNull String name) {
        this.environment = environment;
        this.name = name;

        return this;
    }


    public BaseStateManager withModuleInstance(@NonNull ModuleInstance moduleInstance) {
        this.moduleInstance = moduleInstance;
        return this;
    }

    public BaseStateManager withConfig(@NonNull BaseStateManagerConfig config) {
        this.config = config;
        return this;
    }

    public String basePath() {
        return config().basePath();
    }

    public BaseStateManager init(@NonNull ConnectionManager manger) throws ManagerStateError {
        try {
            Preconditions.checkNotNull(moduleInstance);
            Preconditions.checkNotNull(config);
            Preconditions.checkState(!Strings.isNullOrEmpty(environment));

            connection = manger.getConnection(config.zkConnection(), ZookeeperConnection.class);
            Preconditions.checkNotNull(connection);
            if (!connection.isConnected()) connection.connect();
            CuratorFramework client = connection().client();

            zkPath = PathUtils.formatZkPath(String.format("%s/%s/%s/%s",
                    basePath(),
                    environment,
                    moduleInstance.getModule(),
                    moduleInstance.getName()));
            if (client.checkExists().forPath(zkPath) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkPath);
                if (Strings.isNullOrEmpty(path)) {
                    throw new ManagerStateError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
            }
            zkAgentStatePath = PathUtils.formatZkPath(
                    String.format("%s/%s", zkPath, Constants.ZK_PATH_PROCESS_STATE));
            return this;
        } catch (Exception ex) {
            throw new ManagerStateError(ex);
        }
    }

    public synchronized void checkState() {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
    }

    public void checkAgentState() throws Exception {
        CuratorFramework client = connection().client();

        if (client.checkExists().forPath(zkAgentStatePath) == null) {
            String path = client.create().creatingParentContainersIfNeeded().forPath(zkAgentStatePath);
            if (Strings.isNullOrEmpty(path)) {
                throw new ManagerStateError(String.format("Error creating ZK base path. [path=%s]", basePath()));
            }
            agentTxState = new AgentTxState();
            agentTxState.setNamespace(moduleInstance.getModule());
            agentTxState.setUpdatedTime(0);

            String json = JSONUtils.asString(agentTxState, AgentTxState.class);
            client.setData().forPath(zkAgentStatePath, json.getBytes(StandardCharsets.UTF_8));
        } else {
            agentTxState = readState();
        }
        agentTxState.setModuleInstance(moduleInstance);
        update(agentTxState);
    }

    public AgentTxState initState(long txId) throws ManagerStateError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        if (txId < agentTxState.getProcessedTxId()) return agentTxState;

        synchronized (this) {
            try {
                agentTxState.setProcessedTxId(txId);
                agentTxState.setUpdatedTime(System.currentTimeMillis());

                return update(agentTxState);
            } catch (Exception ex) {
                throw new ManagerStateError(ex);
            }
        }
    }

    private AgentTxState update(AgentTxState agentTxState) throws Exception {
        agentTxState.setUpdatedTime(System.currentTimeMillis());

        CuratorFramework client = connection().client();
        String json = JSONUtils.asString(agentTxState, AgentTxState.class);
        client.setData().forPath(zkAgentStatePath, json.getBytes(StandardCharsets.UTF_8));

        return agentTxState;
    }

    public AgentTxState update(long processedTxId) throws ManagerStateError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        if (processedTxId < agentTxState.getProcessedTxId()) return agentTxState;

        synchronized (this) {
            try {
                agentTxState.setProcessedTxId(processedTxId);
                return update(agentTxState);
            } catch (Exception ex) {
                throw new ManagerStateError(ex);
            }
        }
    }

    private AgentTxState readState() throws ManagerStateError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                byte[] data = client.getData().forPath(zkAgentStatePath);
                if (data != null && data.length > 0) {
                    String json = new String(data, StandardCharsets.UTF_8);
                    return JSONUtils.read(json, AgentTxState.class);
                }
                throw new ManagerStateError(String.format("NameNode State not found. [path=%s]", zkPath));
            } catch (Exception ex) {
                throw new ManagerStateError(ex);
            }
        }
    }


    public Heartbeat heartbeat(@NonNull String name, @NonNull CDCAgentState.AgentState state) throws ManagerStateError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
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
        return PathUtils.formatZkPath(String.format("%s/%s/%s", zkPath, Constants.ZK_PATH_HEARTBEAT, name));
    }

    @Getter
    @Accessors(fluent = true)
    public static abstract class BaseStateManagerConfig extends ConfigReader {

        public static final class Constants {
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
