/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.EFileState;
import ai.sapper.cdc.core.model.dfs.DFSFileInfo;
import ai.sapper.cdc.core.model.dfs.DFSFileReplicaState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.core.model.dfs.DFSReplicationOffset;
import ai.sapper.cdc.core.state.StateManagerError;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import ai.sapper.cdc.entity.model.StaleDataException;
import ai.sapper.cdc.entity.schema.SchemaEntity;
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
    private final HCdcSchemaManager schemaManager;
    private final FileStateHelper fileStateHelper;
    private final NameNodeEnv env;

    public ReplicationStateHelper(@NonNull NameNodeEnv env,
                                  @NonNull FileStateHelper fileStateHelper) {
        this.env = env;
        this.schemaManager = env.schemaManager();
        this.fileStateHelper = fileStateHelper;
    }

    public ReplicationStateHelper withZkConnection(@NonNull ZookeeperConnection connection) {
        this.connection = connection;
        return this;
    }

    public ReplicationStateHelper withZkPath(@NonNull String zkReplicationPath) {
        this.zkReplicationPath = zkReplicationPath;
        return this;
    }

    private String getReplicationPath(SchemaEntity schemaEntity, long inodeId) {
        return new PathUtils.ZkPathBuilder(zkReplicationPath)
                .withPath(schemaEntity.getDomain())
                .withPath(schemaEntity.getEntity())
                .withPath(String.valueOf(inodeId))
                .build();
    }

    public DFSFileReplicaState get(@NonNull SchemaEntity schemaEntity,
                                   long inodeId) throws StateManagerError {
        checkState();
        try {
            CuratorFramework client = connection().client();
            String path = getReplicationPath(schemaEntity, inodeId);
            if (client.checkExists().forPath(path) != null) {
                DFSFileReplicaState state = JSONUtils.read(client, path, DFSFileReplicaState.class);
                if (state != null) {
                    DFSFileState fs = fileStateHelper.get(state.getHdfsPath());
                    if (fs == null) {
                        throw new StateManagerError(
                                String.format("File state not found. [path=%s]", state.getHdfsPath()));
                    }
                    state.setFileInfo(fs.getFileInfo());
                    SchemaEntity entity = schemaManager.getEntity(state.getSchemaDomain(), state.getSchemaEntity());
                    if (entity == null) {
                        throw new StateManagerError(String.format("Schema entity not found. [entity=%s.%s]",
                                state.getSchemaDomain(), state.getSchemaEntity() ));
                    }
                    state.setEntity(entity);
                }
                return state;
            }
            return null;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public DFSFileReplicaState create(@NonNull DFSFileInfo file,
                                      @NonNull SchemaEntity schemaEntity) throws StateManagerError {
        checkState();
        try {
            CuratorFramework client = connection().client();
            DFSFileReplicaState state = get(schemaEntity, file.getInodeId());
            if (state == null) {
                String path = getReplicationPath(schemaEntity, file.getInodeId());
                if (client.checkExists().forPath(path) == null) {
                    client.create().creatingParentContainersIfNeeded().forPath(path);
                }
                state = new DFSFileReplicaState();
                state.setName(String.format("[inode=%d]", file.getInodeId()));
                state.setFileInfo(new DFSFileInfo(file));
                state.setHdfsPath(file.getHdfsPath());
                state.setEntity(schemaEntity);
                state.setSchemaDomain(schemaEntity.getDomain());
                state.setSchemaEntity(schemaEntity.getEntity());
                state.setZkPath(path);
                state.setOffset(new DFSReplicationOffset());
                state.setTimeCreated(System.currentTimeMillis());
                state.setTimeUpdated(System.currentTimeMillis());
                state.setFileState(EFileState.New);
                state.setLastUpdatedBy(env.moduleInstance());
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
            DFSFileReplicaState nstate = get(state.getEntity(), state.getFileInfo().getInodeId());
            if (nstate.getTimeUpdated() > 0 && nstate.getTimeUpdated() != state.getTimeUpdated()) {
                throw new StaleDataException(String.format("Replication state changed. [path=%s]",
                        state.getFileInfo().getHdfsPath()));
            }
            String path = getReplicationPath(state.getEntity(), state.getFileInfo().getInodeId());

            state.setLastUpdatedBy(env.moduleInstance());
            state.setTimeUpdated(System.currentTimeMillis());
            String json = JSONUtils.asString(state, DFSFileReplicaState.class);
            client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

            return state;
        } catch (StaleDataException se) {
            throw se;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public boolean delete(@NonNull SchemaEntity schemaEntity,
                          long inodeId) throws StateManagerError {
        checkState();
        try {
            CuratorFramework client = connection().client();
            DFSFileReplicaState state = get(schemaEntity, inodeId);
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
