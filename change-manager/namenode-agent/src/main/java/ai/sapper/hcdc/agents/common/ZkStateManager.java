package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.filters.DomainManager;
import ai.sapper.cdc.core.model.Heartbeat;
import ai.sapper.cdc.core.model.ModuleInstance;
import ai.sapper.hcdc.agents.model.AgentTxState;
import ai.sapper.hcdc.agents.model.ModuleTxState;
import ai.sapper.hcdc.agents.model.NameNodeAgentState;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;

@Getter
@Accessors(fluent = true)
public class ZkStateManager {
    public static class Constants {
        public static final String ZK_PATH_HEARTBEAT = "/heartbeat";
        public static final String ZK_PATH_FILES = "/files";
        public static final String ZK_PATH_PROCESS_STATE = "state";
        public static final String ZK_PATH_REPLICATION = "/replication";

        public static final String ZK_PATH_TNX_STATE = "/transaction";

        public static final String LOCK_REPLICATION = "LOCK_REPLICATION";
        public static final String ZK_PATH_SNAPSHOT_ID = "snapshotId";
    }

    private ZookeeperConnection connection;
    private ZkStateManagerConfig config;
    private String zkPath;
    private String zkAgentStatePath;
    private String zkModuleStatePath;
    private AgentTxState agentTxState;
    private ModuleTxState moduleTxState;
    private DistributedLock replicationLock;
    private String source;
    private String module;
    private String instance;
    private ModuleInstance moduleInstance;

    private final ReplicationStateHelper replicaStateHelper = new ReplicationStateHelper();
    private final FileStateHelper fileStateHelper = new FileStateHelper();

    public ZkStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull ConnectionManager manger,
                               @NonNull String module,
                               @NonNull String instance) throws StateManagerError {
        try {
            this.instance = instance;
            this.module = module;
            config = new ZkStateManagerConfig(xmlConfig);
            config.read();

            connection = manger.getConnection(config.zkConnection(), ZookeeperConnection.class);
            Preconditions.checkNotNull(connection);
            if (!connection.isConnected()) connection.connect();
            CuratorFramework client = connection().client();

            zkPath = PathUtils.formatZkPath(String.format("%s/%s/%s", basePath(), module, instance));
            if (client.checkExists().forPath(zkPath) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkPath);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
            }

            String zkFSPath = PathUtils.formatZkPath(String.format("%s/%s/%s", basePath(), module, Constants.ZK_PATH_FILES));
            if (client.checkExists().forPath(zkFSPath) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkFSPath);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
            }
            fileStateHelper
                    .withZkPath(zkFSPath)
                    .withZkConnection(connection);
            String zkPathReplication = PathUtils.formatZkPath(
                    String.format("%s/%s/%s", basePath(), module, Constants.ZK_PATH_REPLICATION));
            if (client.checkExists().forPath(zkPathReplication) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkPathReplication);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK replication path. [path=%s]", basePath()));
                }
            }
            moduleTxState = getModuleState();

            replicaStateHelper
                    .withZkConnection(connection)
                    .withZkPath(zkPathReplication);
            return this;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public ZkStateManager withReplicationLock(@NonNull DistributedLock replicationLock) {
        this.replicationLock = replicationLock;
        return this;
    }

    public ZkStateManager withModuleInstance(@NonNull ModuleInstance moduleInstance) {
        this.moduleInstance = moduleInstance;
        return this;
    }

    public ModuleTxState getModuleState() throws Exception {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());

        synchronized (this) {
            if (moduleTxState == null) {
                zkModuleStatePath = PathUtils.formatZkPath(
                        String.format("%s/%s/%s", basePath(), module, Constants.ZK_PATH_PROCESS_STATE));
                moduleTxState = checkModuleState();
                if (moduleTxState == null) {
                    moduleTxState = new ModuleTxState();
                    moduleTxState.setModule(module);

                    moduleTxState = update(moduleTxState);
                }
            }
        }
        return moduleTxState;
    }

    public ModuleTxState updateCurrentTx(long currentTx) throws Exception {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());

        synchronized (this) {
            if (moduleTxState.getCurrentTxId() < currentTx) {
                moduleTxState.setCurrentTxId(currentTx);
                update(moduleTxState);
            }
        }
        return moduleTxState;
    }

    public ModuleTxState updateSnapshotTx(long snapshotTx) throws Exception {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());

        synchronized (this) {
            if (moduleTxState.getSnapshotTxId() < snapshotTx) {
                moduleTxState.setSnapshotTxId(snapshotTx);
                update(moduleTxState);
            }
        }
        return moduleTxState;
    }

    private ModuleTxState update(ModuleTxState state) throws Exception {
        CuratorFramework client = connection().client();
        if (client.checkExists().forPath(zkModuleStatePath) == null) {
            client.create()
                    .creatingParentContainersIfNeeded()
                    .forPath(zkModuleStatePath);
        }
        state.setAgentInstance(instance);
        state.setUpdateTimestamp(System.currentTimeMillis());

        String json = JSONUtils.asString(state, ModuleTxState.class);
        client.setData().forPath(zkModuleStatePath, json.getBytes(StandardCharsets.UTF_8));
        return state;
    }

    private ModuleTxState checkModuleState() throws Exception {
        CuratorFramework client = connection().client();
        if (client.checkExists().forPath(zkModuleStatePath) != null) {
            byte[] data = client.getData().forPath(zkModuleStatePath);
            if (data != null && data.length > 0) {
                return JSONUtils.read(data, ModuleTxState.class);
            }
        }
        return null;
    }

    public void checkAgentState() throws Exception {
        CuratorFramework client = connection().client();
        zkAgentStatePath = PathUtils.formatZkPath(
                String.format("%s/%s", zkPath, Constants.ZK_PATH_PROCESS_STATE));
        if (client.checkExists().forPath(zkAgentStatePath) == null) {
            String path = client.create().creatingParentContainersIfNeeded().forPath(zkAgentStatePath);
            if (Strings.isNullOrEmpty(path)) {
                throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
            }
            agentTxState = new AgentTxState();
            agentTxState.setNamespace(module);
            agentTxState.setUpdatedTime(0);

            String json = JSONUtils.asString(agentTxState, AgentTxState.class);
            client.setData().forPath(zkAgentStatePath, json.getBytes(StandardCharsets.UTF_8));
        } else {
            agentTxState = readState();
        }
        agentTxState.setModuleInstance(moduleInstance);
        update(agentTxState);
    }

    public AgentTxState initState(long txId) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        if (txId < agentTxState.getProcessedTxId()) return agentTxState;

        synchronized (this) {
            try {
                agentTxState.setProcessedTxId(txId);
                agentTxState.setUpdatedTime(System.currentTimeMillis());

                return update(agentTxState);
            } catch (Exception ex) {
                throw new StateManagerError(ex);
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

    public AgentTxState update(long processedTxId) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        if (processedTxId < agentTxState.getProcessedTxId()) return agentTxState;

        synchronized (this) {
            try {
                agentTxState.setProcessedTxId(processedTxId);
                return update(agentTxState);
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    private AgentTxState readState() throws StateManagerError {
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
                throw new StateManagerError(String.format("NameNode State not found. [path=%s]", zkPath));
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    public Heartbeat heartbeat(@NonNull String name, @NonNull NameNodeAgentState.AgentState state) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                String path = getHeartbeatPath(name);
                if (client.checkExists().forPath(path) == null) {
                    path = client.create().creatingParentContainersIfNeeded().forPath(path);
                    if (Strings.isNullOrEmpty(path)) {
                        throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
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
                throw new StateManagerError(ex);
            }
        }
    }

    public Heartbeat heartbeat(@NonNull String name) throws StateManagerError {
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
                    hb.setModule(NameNodeEnv.get().moduleInstance());
                    hb.setState(NameNodeEnv.get().state().state().name());

                    String json = JSONUtils.asString(hb, Heartbeat.class);
                    client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

                    return hb;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    private String getHeartbeatPath(String name) {
        return PathUtils.formatZkPath(String.format("%s/%s/%s", zkPath, Constants.ZK_PATH_HEARTBEAT, name));
    }

    public ModuleTxState updateSnapshotTxId(long txid) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        try {
            return updateSnapshotTx(txid);
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public long getSnapshotTxId() throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        return moduleTxState.getSnapshotTxId();
    }

    public void deleteAll() throws StateManagerError {
        checkState();
        synchronized (this) {
            try {
                fileStateHelper.deleteAll();
                replicaStateHelper.deleteAll();
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }


    private synchronized void checkState() {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
    }

    public String basePath() {
        return config().basePath();
    }

    @Getter
    @Accessors(fluent = true)
    public static class ZkStateManagerConfig extends DomainManager.DomainManagerConfig {
        public static final String __CONFIG_PATH = "managers.state";

        public ZkStateManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
