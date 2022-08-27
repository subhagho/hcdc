package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;

@Getter
@Accessors(fluent = true)
public class ReplicationStateHelper {
    private ZookeeperConnection connection;
    private String zkReplicationPath;

    public ReplicationStateHelper withZkConnection(@NonNull ZookeeperConnection connection) {
        this.connection = connection;
        return this;
    }

    public ReplicationStateHelper withZkPath(@NonNull String zkReplicationPath) {
        this.zkReplicationPath = zkReplicationPath;
        return this;
    }

    public DFSFileReplicaState get(long inodeId) throws StateManagerError {
        checkState();
        try {
            CuratorFramework client = connection().client();
            String path = PathUtils.formatZkPath(String.format("%s/%d", zkReplicationPath, inodeId));
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    String json = new String(data, StandardCharsets.UTF_8);
                    return JSONUtils.read(json, DFSFileReplicaState.class);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public DFSFileReplicaState create(long inodeId,
                                      @NonNull String hdfsPath,
                                      @NonNull SchemaEntity schemaEntity,
                                      boolean enable) throws StateManagerError {
        checkState();
        try {
            CuratorFramework client = connection().client();
            DFSFileReplicaState state = get(inodeId);
            if (state == null) {
                String path = PathUtils.formatZkPath(String.format("%s/%d", zkReplicationPath, inodeId));
                if (client.checkExists().forPath(path) == null) {
                    client.create().creatingParentContainersIfNeeded().forPath(path);
                }
                state = new DFSFileReplicaState();
                state.setInode(inodeId);
                state.setHdfsPath(hdfsPath);
                state.setEntity(schemaEntity);
                state.setZkPath(path);
                state.setEnabled(enable);
                if (enable) {
                    state.setSnapshotTxId(0);
                }
                state.setUpdateTime(System.currentTimeMillis());

                String json = JSONUtils.asString(state, DFSFileReplicaState.class);
                client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
            }
            return state;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public DFSFileReplicaState update(@NonNull DFSFileReplicaState state) throws StateManagerError, StaleDataException {
        checkState();
        try {
            CuratorFramework client = connection().client();
            DFSFileReplicaState nstate = get(state.getInode());
            if (nstate.getUpdateTime() > 0 && nstate.getUpdateTime() != state.getUpdateTime()) {
                throw new StaleDataException(String.format("Replication state changed. [path=%s]", state.getHdfsPath()));
            }
            String path = PathUtils.formatZkPath(String.format("%s/%d", zkReplicationPath, state.getInode()));

            state.setUpdateTime(System.currentTimeMillis());
            String json = JSONUtils.asString(state, DFSFileReplicaState.class);
            client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

            return state;
        } catch (StaleDataException se) {
            throw se;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public boolean delete(long inodeId) throws StateManagerError {
        checkState();
        try {
            CuratorFramework client = connection().client();
            DFSFileReplicaState state = get(inodeId);
            if (state != null) {
                client.delete().deletingChildrenIfNeeded().forPath(state.getZkPath());
                return true;
            }
            return false;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public void deleteAll() throws StateManagerError {
        checkState();
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                if (client.checkExists().forPath(zkReplicationPath) != null) {
                    client.delete().deletingChildrenIfNeeded().forPath(zkReplicationPath);
                }
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    private synchronized void checkState() {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
    }
}
