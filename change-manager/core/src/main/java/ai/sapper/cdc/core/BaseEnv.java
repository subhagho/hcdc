package ai.sapper.cdc.core;

import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public abstract class BaseEnv {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class LockDef {
        private String module;
        private String path;
        private ZookeeperConnection connection;
    }

    public static class Constants {
        private static final String CONFIG_LOCKS = "locks";
        private static final String CONFIG_LOCK = String.format("%s.lock", CONFIG_LOCKS);
        private static final String CONFIG_LOCK_NAME = "name";
        private static final String CONFIG_LOCK_CONN = "connection";
        private static final String CONFIG_LOCK_NODE = "lock-node";
    }

    private ConnectionManager connectionManager;
    private final Map<String, LockDef> lockDefs = new HashMap<>();

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                     @NonNull String module,
                     @NonNull String connectionsConfigPath) throws ConfigurationException {
        try {
            connectionManager = new ConnectionManager();
            connectionManager.init(xmlConfig, connectionsConfigPath);

            readLocks(xmlConfig, module);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void readLocks(HierarchicalConfiguration<ImmutableNode> configNode, String module) throws Exception {
        List<HierarchicalConfiguration<ImmutableNode>> nodes = configNode.configurationsAt(Constants.CONFIG_LOCK);
        for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
            String name = node.getString(Constants.CONFIG_LOCK_NAME);
            String conn = node.getString(Constants.CONFIG_LOCK_CONN);
            String path = name;
            if (node.containsKey(Constants.CONFIG_LOCK_NODE)) {
                path = node.getString(Constants.CONFIG_LOCK_NODE);
            }
            ZookeeperConnection connection = connectionManager.getConnection(conn, ZookeeperConnection.class);
            LockDef def = new LockDef()
                    .module(module)
                    .path(path)
                    .connection(connection);

            lockDefs.put(name, def);
        }
    }

    public void close() throws Exception {
        if (connectionManager != null) {
            connectionManager.close();
        }
    }
}
