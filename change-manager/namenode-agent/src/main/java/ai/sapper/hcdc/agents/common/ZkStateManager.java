package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseStateManager;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.ManagerStateError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.model.Heartbeat;
import ai.sapper.hcdc.agents.model.ModuleTxState;
import ai.sapper.hcdc.agents.model.SnapshotState;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;

@Getter
@Accessors(fluent = true)
public class ZkStateManager extends BaseStateManager {
    public static class Constants {
        public static final String ZK_PATH_FILES = "/files";
        public static final String ZK_PATH_REPLICATION = "/replication";
    }

    private String source;
    private String zkModuleStatePath;
    private ModuleTxState moduleTxState;
    private String zkSnapshotStatePath;
    private SnapshotState snapshotState;

    @Getter(AccessLevel.NONE)
    private DistributedLock replicationLock;

    private final ReplicationStateHelper replicaStateHelper = new ReplicationStateHelper();
    private final FileStateHelper fileStateHelper = new FileStateHelper();


    public ZkStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull ConnectionManager manger,
                               @NonNull String source) throws StateManagerError {
        Preconditions.checkState(!Strings.isNullOrEmpty(name()));
        try {
            ZkStateManagerConfig config = new ZkStateManagerConfig(xmlConfig);
            config.read();

            withConfig(config);
            super.init(manger);

            this.source = source;
            CuratorFramework client = connection().client();
            String zkFSPath = new PathUtils.ZkPathBuilder(zkModulePath())
                    .withPath(Constants.ZK_PATH_FILES)
                    .withPath(source)
                    .build();
            if (client.checkExists().forPath(zkFSPath) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkFSPath);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
            }
            fileStateHelper
                    .withZkPath(zkFSPath)
                    .withZkConnection(connection());
            String zkPathReplication = new PathUtils.ZkPathBuilder(zkModulePath())
                    .withPath(Constants.ZK_PATH_REPLICATION)
                    .withPath(source)
                    .build();
            if (client.checkExists().forPath(zkPathReplication) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(zkPathReplication);
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK replication path. [path=%s]", basePath()));
                }
            }
            moduleTxState = getModuleState();
            snapshotState = getSnapshotState();

            replicaStateHelper
                    .withZkConnection(connection())
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

    public long nextSnapshotSeq() throws Exception {
        checkState();
        synchronized (this) {
            long seq = snapshotState.getSnapshotSeq();
            snapshotState.setSnapshotSeq(seq + 1);
            save(snapshotState);
            return seq;
        }
    }

    public SnapshotState getSnapshotState() throws Exception {
        checkState();
        synchronized (this) {
            if (snapshotState == null) {
                zkSnapshotStatePath = new PathUtils.ZkPathBuilder(zkModulePath())
                        .withPath("snapshot")
                        .build();
                snapshotState = checkSnapshotState();
                if (snapshotState == null) {
                    snapshotState = new SnapshotState();
                    snapshotState.setModule(moduleInstance().getModule());
                    snapshotState.setSnapshotSeq(0);
                    snapshotState = save(snapshotState);
                }
            }
        }
        return snapshotState;
    }

    private SnapshotState save(SnapshotState state) throws Exception {
        CuratorFramework client = connection().client();
        if (client.checkExists().forPath(zkSnapshotStatePath) == null) {
            client.create().creatingParentsIfNeeded().forPath(zkSnapshotStatePath);
        }
        state.setUpdatedTimestamp(System.currentTimeMillis());
        String json = JSONUtils.asString(state, SnapshotState.class);
        client.setData().forPath(zkSnapshotStatePath, json.getBytes(StandardCharsets.UTF_8));
        return state;
    }

    private SnapshotState checkSnapshotState() throws Exception {
        CuratorFramework client = connection().client();
        if (client.checkExists().forPath(zkSnapshotStatePath) != null) {
            byte[] data = client.getData().forPath(zkSnapshotStatePath);
            if (data != null && data.length > 0) {
                snapshotState = JSONUtils.read(data, SnapshotState.class);
                return snapshotState;
            }
        }
        return null;
    }

    public ModuleTxState getModuleState() throws Exception {
        checkState();
        synchronized (this) {
            if (moduleTxState == null) {
                zkModuleStatePath = new PathUtils.ZkPathBuilder(zkModulePath())
                        .withPath(BaseStateManager.Constants.ZK_PATH_PROCESS_STATE)
                        .build();
                moduleTxState = checkModuleState();
                if (moduleTxState == null) {
                    moduleTxState = new ModuleTxState();
                    moduleTxState.setModule(moduleInstance().getModule());

                    moduleTxState = update(moduleTxState);
                }
            }
        }
        return moduleTxState;
    }

    public ModuleTxState updateCurrentTx(long currentTx) throws Exception {
        checkState();
        synchronized (this) {
            if (moduleTxState.getCurrentTxId() < currentTx) {
                moduleTxState.setCurrentTxId(currentTx);
                update(moduleTxState);
            }
        }
        return moduleTxState;
    }

    public ModuleTxState updateSnapshotTx(long snapshotTx) throws Exception {
        checkState();
        synchronized (this) {
            if (moduleTxState.getSnapshotTxId() < snapshotTx) {
                moduleTxState.setSnapshotTxId(snapshotTx);
                update(moduleTxState);
            }
            if (snapshotState.getSnapshotTxId() < snapshotTx) {
                snapshotState.setSnapshotTxId(snapshotTx);
                snapshotState = save(snapshotState);
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
        state.setAgentInstance(moduleInstance().getName());
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

    public ModuleTxState updateSnapshotTxId(long txid) throws StateManagerError {
        checkState();
        try {
            return updateSnapshotTx(txid);
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public long getSnapshotTxId() throws StateManagerError {
        checkState();
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


    public String basePath() {
        return config().basePath();
    }

    @Override
    public Heartbeat heartbeat(@NonNull String name) throws ManagerStateError {
        try {
            return heartbeat(name, NameNodeEnv.get(name).agentState());
        } catch (Exception ex) {
            throw new ManagerStateError(ex);
        }
    }

    public void stateLock() {
        replicationLock.lock();
    }

    public void stateUnlock() {
        replicationLock.unlock();
    }

    @Getter
    @Accessors(fluent = true)
    public static class ZkStateManagerConfig extends BaseStateManager.BaseStateManagerConfig {
        public static final String __CONFIG_PATH = "managers.state";

        public ZkStateManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
