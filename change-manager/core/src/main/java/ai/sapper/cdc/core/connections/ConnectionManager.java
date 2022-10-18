package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.connections.settngs.ESettingsSource;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ai.sapper.cdc.common.utils.DefaultLogger.stacktrace;

public class ConnectionManager implements Closeable {
    public static Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    public static class Constants {
        public static final String __CONFIG_PATH = "connections";
        public static final String CONFIG_CONNECTION_LIST = "connection";
        public static final String CONFIG_TYPE = "type";
        public static final String CONFIG_SHARED = "shared";
        public static final String CONFIG_SHARED_ZK = String.format("%s.connection", CONFIG_SHARED);
        public static final String CONFIG_SHARED_ZK_PATH = String.format("%s.path", CONFIG_SHARED);
        public static final String PATH_ZK_CLASS = "class";
        public static final String CONFIG_SAVE_CONNECTIONS = "save";
        public static final String CONFIG_OVERRIDE_FROM_FILE = "override";
    }

    private String configPath;
    private HierarchicalConfiguration<ImmutableNode> config;
    private final Map<String, Connection> connections = new HashMap<>();
    private ZookeeperConnection connection;
    private String zkPath;
    private KeyStore keyStore;
    private String environment;
    private boolean saveByDefault = false;
    private boolean overrideFromFile = true;
    private BaseEnv<?> env;

    public ConnectionManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                  @NonNull BaseEnv<?> env,
                                  String pathPrefix) throws ConnectionError {
        if (Strings.isNullOrEmpty(pathPrefix)) {
            configPath = Constants.__CONFIG_PATH;
        } else {
            configPath = String.format("%s.%s", pathPrefix, Constants.__CONFIG_PATH);
        }
        this.env = env;
        this.environment = env.environment();
        try {

            config = xmlConfig.configurationAt(configPath);
            String s = config.getString(Constants.CONFIG_SAVE_CONNECTIONS);
            if (!Strings.isNullOrEmpty(s)) {
                saveByDefault = Boolean.parseBoolean(s);
            }
            s = config.getString(Constants.CONFIG_OVERRIDE_FROM_FILE);
            if (!Strings.isNullOrEmpty(s)) {
                overrideFromFile = Boolean.parseBoolean(s);
            }
            synchronized (connections) {
                int count = initConnections();
                count += initSharedConnections();
                LOG.info(String.format("Initialized %d connections...", count));
                for (String name : connections.keySet()) {
                    Connection conn = connections.get(name);
                    if (conn.settings().getSource() == ESettingsSource.File
                            && saveByDefault) {
                        save(conn);
                    }
                }
            }
            return this;
        } catch (Exception ex) {
            connections.clear();
            stacktrace(LOG, ex);
            throw new ConnectionError("Error Initializing connections.", ex);
        }
    }

    public ConnectionManager withKeyStore(KeyStore keyStore) {
        this.keyStore = keyStore;
        return this;
    }

    public KeyStore keyStore() {
        return keyStore;
    }

    private int initSharedConnections() throws Exception {
        int count = 0;
        if (ConfigReader.checkIfNodeExists(config, Constants.CONFIG_SHARED)) {
            String zk = config.getString(Constants.CONFIG_SHARED_ZK);
            if (Strings.isNullOrEmpty(zk)) {
                throw new Exception(
                        String.format("ZooKeeper connection name not found. [path=%s]", Constants.CONFIG_SHARED_ZK));
            }
            connection = getConnection(zk, ZookeeperConnection.class);
            if (connection == null) {
                throw new Exception(String.format("ZooKeeper connection not found. [name=%s]", zk));
            }
            String bp = config.getString(Constants.CONFIG_SHARED_ZK_PATH);

            String path = new PathUtils.ZkPathBuilder(bp)
                    .withPath(environment)
                    .withPath(Constants.__CONFIG_PATH)
                    .build();
            if (Strings.isNullOrEmpty(path)) {
                throw new Exception(
                        String.format("ZooKeeper path not found. [path=%s]", Constants.CONFIG_SHARED_ZK_PATH));
            }
            if (!connection.isConnected()) connection.connect();
            CuratorFramework client = connection.client();
            if (client.checkExists().forPath(path) != null) {
                List<String> types = client.getChildren().forPath(path);
                if (types != null && !types.isEmpty()) {
                    for (String type : types) {
                        String tp = new PathUtils.ZkPathBuilder(path)
                                .withPath(type)
                                .build();
                        List<String> names = client.getChildren().forPath(tp);
                        if (names != null && !names.isEmpty()) {
                            for (String name : names) {
                                if (connections.containsKey(name) && overrideFromFile) {
                                    continue;
                                }
                                String cp = new PathUtils.ZkPathBuilder(tp)
                                        .withPath(name)
                                        .build();
                                initConnection(connection, cp, name);
                                count++;
                            }
                        }
                    }
                }
            }
            zkPath = path;
        }
        return count;
    }

    @SuppressWarnings("unchecked")
    private void initConnection(ZookeeperConnection zkc, String path, String name) throws Exception {
        CuratorFramework client = zkc.client();
        String cp = new PathUtils.ZkPathBuilder(path)
                .withPath(Constants.PATH_ZK_CLASS)
                .build();
        if (client.checkExists().forPath(cp) == null) {
            throw new Exception(String.format("Implementing class path not found. [path=%s]", cp));
        }
        byte[] data = client.getData().forPath(cp);
        if (data == null || data.length <= 0) {
            throw new Exception(String.format("Implementing class not found. [path=%s]", cp));
        }
        String cls = new String(data, StandardCharsets.UTF_8);
        Class<? extends Connection> cClass = (Class<? extends Connection>) Class.forName(cls);
        Connection connection = cClass.getDeclaredConstructor().newInstance();

        connection.init(name, zkc, path, env);

        addConnection(connection.name(), connection);
    }

    private int initConnections() throws Exception {
        if (ConfigReader.checkIfNodeExists(config, Constants.CONFIG_CONNECTION_LIST)) {
            List<HierarchicalConfiguration<ImmutableNode>> nodes = config.configurationsAt(Constants.CONFIG_CONNECTION_LIST);
            if (!nodes.isEmpty()) {
                for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                    Connection connection = initConnection(node);
                    LOG.info(String.format("Initialized connection: [name=%s][type=%s]...", connection.name(), connection.getClass().getCanonicalName()));
                }
            }
        }
        return connections.size();
    }

    @SuppressWarnings("unchecked")
    private Connection initConnection(HierarchicalConfiguration<ImmutableNode> node) throws Exception {
        String type = node.getString(Constants.CONFIG_TYPE);
        if (Strings.isNullOrEmpty(type)) {
            throw new ConnectionError(String.format("Connection type not found. [node=%s]", node.toString()));
        }
        Class<? extends Connection> cls = (Class<? extends Connection>) Class.forName(type);
        Connection connection = cls.getDeclaredConstructor().newInstance();
        connection.init(node, env);
        Preconditions.checkState(!Strings.isNullOrEmpty(connection.name()));
        Preconditions.checkState(connection.connectionState() == Connection.EConnectionState.Initialized);

        addConnection(connection.name(), connection);
        LOG.info(String.format("Initialized connection [type=%s][name=%s]", connection.getClass().getCanonicalName(), connection.name()));
        return connection;
    }

    public Connection getConnection(@NonNull String name) {
        return connections.get(name);
    }

    @SuppressWarnings("unchecked")
    public <T extends Connection> T getConnection(@NonNull String name,
                                                  @NonNull Class<? extends Connection> type) {
        Connection connection = getConnection(name);
        if (connection != null
                && (connection.getClass().equals(type) || ReflectionUtils.isSuperType(type, connection.getClass()))) {
            return (T) connection;
        }
        return null;
    }

    public void addConnection(@NonNull String name, @NonNull Connection connection) throws ConnectionError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        if (connections.containsKey(name))
            LOG.warn(String.format("Connection with name already exists. [name=%s]", name));
        connections.put(name, connection);
    }

    @Override
    public void close() throws IOException {
        if (!connections.isEmpty()) {
            for (String key : connections.keySet()) {
                Connection c = connections.get(key);
                if (c != null) {
                    c.close();
                }
            }
            connections.clear();
        }
    }

    public void save(@NonNull Connection connection) throws ConnectionError {
        Preconditions.checkNotNull(this.connection);
        try {
            String basePath = new PathUtils.ZkPathBuilder(zkPath)
                    .withPath(connection.type().name())
                    .withPath(connection.name())
                    .build();
            CuratorFramework client = this.connection.client();
            String path = new PathUtils.ZkPathBuilder(basePath)
                    .withPath(connection.path())
                    .build();
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
            connection.settings().setSource(ESettingsSource.ZooKeeper);
            String json = JSONUtils.asString(connection.settings(), connection.settings().getClass());
            client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
            path = new PathUtils.ZkPathBuilder(basePath)
                    .withPath(Constants.PATH_ZK_CLASS)
                    .build();
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
            connection.settings().setSource(ESettingsSource.ZooKeeper);
            client.setData().forPath(path,
                    connection.getClass().getCanonicalName().getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
    }

    public boolean delete(@NonNull Connection connection) throws ConnectionError {
        Preconditions.checkNotNull(this.connection);
        boolean ret = false;
        try {
            String basePath = new PathUtils.ZkPathBuilder(zkPath)
                    .withPath(connection.type().name())
                    .withPath(connection.name())
                    .build();
            CuratorFramework client = this.connection.client();
            if (client.checkExists().forPath(basePath) != null) {
                connection.close();
                client.delete().deletingChildrenIfNeeded().forPath(basePath);
                connections.remove(connection.name());
                ret = true;
            }
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
        return ret;
    }

    public void save() throws ConnectionError {
        for (String name : connections.keySet()) {
            Connection connection = connections.get(name);
            save(connection);
            DefaultLogger.LOGGER.info(
                    String.format("Saved connection: [name=%s][type=%s]",
                            name, connection.getClass().getCanonicalName()));
        }
    }

    public void createOrUpdate(@NonNull Class<? extends Connection> type,
                               @NonNull ConnectionSettings settings) throws ConnectionError {
        synchronized (connections) {
            try {
                try (Connection connection = type
                        .getDeclaredConstructor()
                        .newInstance()
                        .setup(settings, env)) {
                    connection.connect();
                    addConnection(connection.name(), connection);
                    save(connection);
                }
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    public boolean remove(@NonNull Class<? extends Connection> type,
                          @NonNull String name) throws ConnectionError {
        synchronized (connections) {
            Connection connection = getConnection(name, type);
            if (connection != null) {
                return delete(connection);
            }
            return false;
        }
    }

    public Map<String, ConnectionSettings> list(String type) {
        if (!connections.isEmpty()) {
            EConnectionType ct = EConnectionType.parse(type);
            Map<String, ConnectionSettings> settings = new HashMap<>();
            for (String name : connections.keySet()) {
                Connection connection = connections.get(name);
                if (ct == null || connection.type() == ct) {
                    settings.put(connection.name(), connection.settings());
                }
            }
            if (!settings.isEmpty()) return settings;
        }
        return null;
    }
}
