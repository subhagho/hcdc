package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.agents.namenode.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.namenode.model.NameNodeAgentState;
import ai.sapper.hcdc.agents.namenode.model.NameNodeTxState;
import ai.sapper.hcdc.common.model.DFSError;
import ai.sapper.hcdc.common.model.SchemaEntity;
import ai.sapper.hcdc.common.utils.JSONUtils;
import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.DistributedLock;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import ai.sapper.hcdc.core.filters.DomainManager;
import ai.sapper.hcdc.core.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class ZkStateManager {
    public static class Constants {
        public static final String ZK_PATH_HEARTBEAT = "/heartbeat";
        public static final String ZK_PATH_FILES = "/files";
        public static final String ZK_PATH_PROCESS_STATE = "state";
        public static final String ZK_PATH_REPLICATION = "/replication";

        public static final String LOCK_REPLICATION = "LOCK_REPLICATION";
        public static final String ZK_PATH_SNAPSHOT_ID = "snapshotId";
    }

    private ZookeeperConnection connection;
    private ZkStateManagerConfig config;
    private String zkPath;
    private String zkStatePath;
    private NameNodeTxState agentTxState;
    private DistributedLock replicationLock;
    private String module;
    private String instance;

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
            checkAgentState();
            String zkPathReplication = PathUtils.formatZkPath(
                    String.format("%s/%s/%s", basePath(), module, Constants.ZK_PATH_REPLICATION));
            if (client.checkExists().forPath(zkPathReplication) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkPathReplication);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK replication path. [path=%s]", basePath()));
                }
            }
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

    private void checkAgentState() throws Exception {
        CuratorFramework client = connection().client();
        zkStatePath = PathUtils.formatZkPath(
                String.format("%s/%s", zkPath, Constants.ZK_PATH_PROCESS_STATE));
        if (client.checkExists().forPath(zkStatePath) == null) {
            String path = client.create().creatingParentContainersIfNeeded().forPath(zkStatePath);
            if (Strings.isNullOrEmpty(path)) {
                throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
            }
            agentTxState = new NameNodeTxState();
            agentTxState.setNamespace(module);
            agentTxState.setUpdatedTime(0);

            String json = JSONUtils.asString(agentTxState, NameNodeTxState.class);
            client.setData().forPath(zkStatePath, json.getBytes(StandardCharsets.UTF_8));
        } else {
            agentTxState = readState();
        }
    }

    public NameNodeTxState initState(long txId) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        Preconditions.checkArgument(txId > agentTxState.getProcessedTxId());

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

    private NameNodeTxState update(NameNodeTxState agentTxState) throws Exception {
        agentTxState.setUpdatedTime(System.currentTimeMillis());

        CuratorFramework client = connection().client();
        String json = JSONUtils.asString(agentTxState, NameNodeTxState.class);
        client.setData().forPath(zkStatePath, json.getBytes(StandardCharsets.UTF_8));

        return agentTxState;
    }

    public NameNodeTxState update(long processedTxId) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        Preconditions.checkArgument(processedTxId > agentTxState.getProcessedTxId());

        synchronized (this) {
            try {
                agentTxState.setProcessedTxId(processedTxId);
                return update(agentTxState);
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    private NameNodeTxState readState() throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                byte[] data = client.getData().forPath(zkStatePath);
                if (data != null && data.length > 0) {
                    String json = new String(data, StandardCharsets.UTF_8);
                    return JSONUtils.read(json, NameNodeTxState.class);
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

    public String updateSnapshotTxId(long txid) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        try {
            CuratorFramework client = connection().client();
            String path = getSnapshotIDPath();
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentContainersIfNeeded().forPath(path);
            }
            byte[] data = String.valueOf(txid).getBytes(StandardCharsets.UTF_8);
            client.setData().forPath(path, data);
            return path;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public long getSnapshotTxId() throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        try {
            CuratorFramework client = connection().client();
            String path = getSnapshotIDPath();
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    String v = new String(data, StandardCharsets.UTF_8);
                    return Long.parseLong(v);
                }
            }
            return -1;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    private String getSnapshotIDPath() {
        return PathUtils.formatZkPath(String.format("%s/%s/%s", basePath(), module, Constants.ZK_PATH_SNAPSHOT_ID));
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

        private static final String __CONFIG_PATH = "state.manager";
        private static final String CONFIG_MODULE_NAME = "module";

        public ZkStateManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
