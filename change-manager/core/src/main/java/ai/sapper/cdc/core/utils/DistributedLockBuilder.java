package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
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
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;
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
        private static final String CONFIG_ZK_NODE_PATH = String.format("%s.path", CONFIG_LOCKS);
        private static final String CONFIG_LOCK = String.format("%s.lock", CONFIG_LOCKS);
        private static final String CONFIG_LOCK_NAME = "name";
        private static final String CONFIG_LOCK_NODE = "lock-node";
    }

    private ZookeeperConnection connection;
    private String environment;
    private String zkPath;
    private final Map<String, LockDef> lockDefs = new HashMap<>();

    public DistributedLockBuilder withEnv(@NonNull String environment) {
        this.environment = environment;
        return this;
    }

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
            String path = name;
            if (node.containsKey(Constants.CONFIG_LOCK_NODE)) {
                path = node.getString(Constants.CONFIG_LOCK_NODE);
            }
            LockDef def = new LockDef();
            def.setName(name);
            def.setModule(module);
            def.setPath(path);

            lockDefs.put(name, def);
        }
    }

    private void readLocks(HierarchicalConfiguration<ImmutableNode> configNode) throws Exception {
        zkPath = configNode.getString(Constants.CONFIG_ZK_NODE_PATH);
        if (Strings.isNullOrEmpty(zkPath)) return;

        zkPath = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(Constants.CONFIG_LOCKS)
                .withPath(environment)
                .build();
        CuratorFramework client = connection.client();
        if (client.checkExists().forPath(zkPath) == null) return;
        List<String> modules = client.getChildren().forPath(zkPath);
        if (modules != null && !modules.isEmpty()) {
            for (String c : modules) {
                String mp = new PathUtils.ZkPathBuilder(zkPath)
                        .withPath(c)
                        .build();
                List<String> locks = client.getChildren().forPath(mp);
                if (locks != null && !locks.isEmpty()) {
                    for (String lc : locks) {
                        String lp = new PathUtils.ZkPathBuilder(mp)
                                .withPath(lc)
                                .build();
                        byte[] data = client.getData().forPath(lp);
                        if (data != null && data.length > 0) {
                            LockDef def = JSONUtils.read(data, LockDef.class);
                            lockDefs.put(def.getName(), def);
                        }
                    }
                }
            }
        }
    }

    public DistributedLock createLock(@NonNull String path, @NonNull String name) throws Exception {
        if (lockDefs().containsKey(name)) {
            LockDef def = lockDefs().get(name);
            if (def == null) {
                throw new Exception(String.format("No lock definition found: [name=%s]", name));
            }
            return new DistributedLock(def.getModule(),
                    def.getPath(),
                    path)
                    .withConnection(connection);
        }
        return null;
    }

    public void save(@NonNull LockDef def) throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(zkPath));
        CuratorFramework client = connection.client();
        String path = new PathUtils.ZkPathBuilder(zkPath)
                .withPath(def.getModule())
                .withPath(def.getName())
                .build();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        String json = JSONUtils.asString(def, LockDef.class);
        client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
    }

    public void save() throws Exception {
        if (!lockDefs.isEmpty()) {
            for (String name : lockDefs.keySet()) {
                save(lockDefs.get(name));
            }
        }
    }
}
