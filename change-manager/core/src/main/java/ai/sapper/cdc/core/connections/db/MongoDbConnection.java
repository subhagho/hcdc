package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.connections.settngs.MongoDbConnectionSettings;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.google.common.base.Preconditions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class MongoDbConnection implements Connection {
    @Getter(AccessLevel.NONE)
    protected final Connection.ConnectionState state = new Connection.ConnectionState();
    protected ConnectionManager connectionManager;
    protected MongoDbConnectionSettings settings;
    private MongoClient client;

    protected final String zkNode;
    private MongoDbConnectionConfig config;

    public MongoDbConnection() {
        this.zkNode = MongoDbConnectionConfig.Constants.__CONFIG_PATH;
    }

    @Override
    public String name() {
        return settings.getName();
    }

    public MongoClient client() {
        Preconditions.checkState(state.isConnected());
        return client;
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                this.connectionManager = env.connectionManager();
                config = new MongoDbConnectionConfig(xmlConfig);
                settings = config.read();

                state.state(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                CuratorFramework client = connection.client();
                String hpath = new PathUtils.ZkPathBuilder(path)
                        .withPath(zkNode)
                        .build();
                if (client.checkExists().forPath(hpath) == null) {
                    throw new Exception(String.format("JDBC Settings path not found. [path=%s]", hpath));
                }
                byte[] data = client.getData().forPath(hpath);
                settings = JSONUtils.read(data, MongoDbConnectionSettings.class);
                Preconditions.checkNotNull(settings);
                Preconditions.checkState(name.equals(settings.getName()));
                this.connectionManager = env.connectionManager();
                state.state(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                this.settings = (MongoDbConnectionSettings) settings;
                this.connectionManager = env.connectionManager();
                state.state(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection connect() throws ConnectionError {
        KeyStore keyStore = connectionManager().keyStore();
        Preconditions.checkNotNull(keyStore);
        synchronized (state) {
            if (state.isConnected()) return this;
            Preconditions.checkState(state.state() == EConnectionState.Initialized);
            try {
                String url = createConnectionUrl(keyStore);
                client = MongoClients.create(url);
                state.state(EConnectionState.Connected);
                return this;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    protected String createConnectionUrl(KeyStore keyStore) throws Exception {
        String url = "mongodb://";
        StringBuilder builder = new StringBuilder(url);
        builder.append(settings.getUser());
        String pk = settings.getPassword();
        builder.append(":")
                .append(keyStore.read(pk));
        builder.append("@")
                .append(settings.getHost())
                .append(":")
                .append(settings.getPort());
        builder.append("/")
                .append(settings.getDb());
        builder.append("/?")
                .append(DbConnection.Constants.DB_KEY_POOL_SIZE)
                .append(settings.getPoolSize());
        if (settings.getParameters() != null && !settings.getParameters().isEmpty()) {
            for (String param : settings.getParameters().keySet()) {
                builder.append("&")
                        .append(param)
                        .append("=")
                        .append(settings.getParameters().get(param));
            }
        }
        return builder.toString();
    }

    @Override
    public Throwable error() {
        return state.error();
    }

    @Override
    public EConnectionState connectionState() {
        return state.state();
    }

    @Override
    public boolean isConnected() {
        return state.isConnected();
    }

    @Override
    public String path() {
        return null;
    }

    @Override
    public ConnectionSettings settings() {
        return settings;
    }

    @Override
    public EConnectionType type() {
        return EConnectionType.db;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            synchronized (state) {
                if (client != null) {
                    client.close();
                    client = null;
                }
            }
        }
        state.state(EConnectionState.Closed);
    }
}
