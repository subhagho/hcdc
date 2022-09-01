package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseStateManager;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.filters.DomainManager;
import ai.sapper.hcdc.agents.model.ModuleTxState;
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
public class ZkStateManager extends BaseStateManager {
    public static class Constants {
        public static final String ZK_PATH_FILES = "/files";
        public static final String ZK_PATH_REPLICATION = "/replication";
    }

    private String source;
    private String zkModuleStatePath;
    private ModuleTxState moduleTxState;
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

    @Getter
    @Accessors(fluent = true)
    public static class ZkStateManagerConfig extends DomainManager.DomainManagerConfig {
        public static final String __CONFIG_PATH = "managers.state";

        public ZkStateManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
