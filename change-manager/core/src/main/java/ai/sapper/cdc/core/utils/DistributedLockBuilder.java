package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.LockDef;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class DistributedLockBuilder {
    public static class Constants {
        private static final String CONFIG_LOCKS = "locks";
        private static final String CONFIG_ZK_CONN = String.format("%s.connection", CONFIG_LOCKS);
        private static final String CONFIG_ZK_NODE = String.format("%s.path", CONFIG_LOCKS);
        private static final String CONFIG_LOCK = String.format("%s.lock", CONFIG_LOCKS);
        private static final String CONFIG_LOCK_NAME = "name";
        private static final String CONFIG_LOCK_CONN = "connection";
        private static final String CONFIG_LOCK_NODE = "lock-node";
    }

    private ZookeeperConnection connection;
    private String zkPath;
    private final Map<String, LockDef> lockDefs = new HashMap<>();

    public DistributedLockBuilder init(@NonNull HierarchicalConfiguration<ImmutableNode> configNode,
                                       @NonNull String module,
                                       @NonNull ConnectionManager connectionManager) throws Exception {
        String zkConn = configNode.getString(Constants.CONFIG_ZK_CONN);
        if (Strings.isNullOrEmpty(zkConn)) {
            throw new Exception(String.format("ZooKeeper connection not defined. [path=%s]", Constants.CONFIG_ZK_CONN));
        }
        connection = connectionManager.getConnection(zkConn, ZookeeperConnection.class);
        Preconditions.checkNotNull(connection);
        if (!connection.isConnected()) connection.connect();

        readLocks(configNode, module);
        readLocks(configNode);
        return this;
    }

    private void readLocks(HierarchicalConfiguration<ImmutableNode> configNode,
                           String module) throws Exception {
        List<HierarchicalConfiguration<ImmutableNode>> nodes = configNode.configurationsAt(Constants.CONFIG_LOCK);
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            String name = node.getString(Constants.CONFIG_LOCK_NAME);
            String conn = node.getString(Constants.CONFIG_LOCK_CONN);
            String path = name;
            if (node.containsKey(Constants.CONFIG_LOCK_NODE)) {
                path = node.getString(Constants.CONFIG_LOCK_NODE);
            }
            LockDef def = new LockDef();
            def.setModule(module);
            def.setPath(path);

            lockDefs.put(name, def);
        }
    }

    private void readLocks(HierarchicalConfiguration<ImmutableNode> configNode) throws Exception {
        zkPath = configNode.getString(Constants.CONFIG_ZK_NODE);
        if (Strings.isNullOrEmpty(zkPath)) return;
        zkPath = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(Constants.CONFIG_LOCKS)
                .build();
        CuratorFramework client = connection.client();
        if (client.checkExists().forPath(zkPath) == null) return;
        List<String> modules = client.getChildren().forPath(zkPath);
        if (modules != null && !modules.isEmpty()) {
            for (String c : modules) {

            }
        }
    }
}
