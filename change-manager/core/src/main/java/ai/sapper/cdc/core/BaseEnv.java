package ai.sapper.cdc.core;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import com.google.common.base.Strings;
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
    private String storeKey;
    private KeyStore keyStore;

    public BaseEnv withStoreKey(@NonNull String storeKey) {
        this.storeKey = storeKey;
        return this;
    }

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                     @NonNull String module,
                     @NonNull String connectionsConfigPath) throws ConfigurationException {
        try {
            if (ConfigReader.checkIfNodeExists(xmlConfig, KeyStore.__CONFIG_PATH)) {
                String c = xmlConfig.getString(KeyStore.CONFIG_KEYSTORE_CLASS);
                if (Strings.isNullOrEmpty(c)) {
                    throw new ConfigurationException(
                            String.format("Key Store class not defined. [config=%s]", KeyStore.CONFIG_KEYSTORE_CLASS));
                }
                Class<? extends KeyStore> cls = (Class<? extends KeyStore>) Class.forName(c);
                keyStore = cls.getDeclaredConstructor().newInstance();
                keyStore.withPassword(storeKey)
                        .init(xmlConfig);
            }
            this.storeKey = null;

            connectionManager = new ConnectionManager()
                    .withKeyStore(keyStore);
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


    public DistributedLock createLock(@NonNull String path, @NonNull String name) throws Exception {
        if (lockDefs().containsKey(name)) {
            LockDef def = lockDefs().get(name);
            if (def == null) {
                throw new Exception(String.format("No lock definition found: [name=%s]", name));
            }
            return new DistributedLock(def.module(),
                    def.path(),
                    path)
                    .withConnection(def.connection());
        }
        return null;
    }

    public void close() throws Exception {
        if (connectionManager != null) {
            connectionManager.close();
        }
    }
}
