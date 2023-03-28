package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.*;
import ai.sapper.cdc.core.connections.settngs.AzureTableConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.keystore.KeyStore;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class AzureTableConnection implements Connection {
    private AzureTableConnectionConfig config;
    private AzureTableConnectionSettings settings;
    protected ConnectionManager connectionManager;
    private TableServiceClient client;
    private final ConnectionState state = new ConnectionState();

    @Override
    public String name() {
        return settings.getName();
    }

    public String db() {
        return settings.getDb();
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
                config = new AzureTableConnectionConfig(xmlConfig);
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
                           @NonNull String path, @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                CuratorFramework client = connection.client();
                String hpath = new PathUtils.ZkPathBuilder(path)
                        .withPath(AzureTableConnectionConfig.__CONFIG_PATH)
                        .build();
                if (client.checkExists().forPath(hpath) == null) {
                    throw new Exception(String.format("JDBC Settings path not found. [path=%s]", hpath));
                }
                byte[] data = client.getData().forPath(hpath);
                settings = JSONUtils.read(data, AzureTableConnectionSettings.class);
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
                this.settings = (AzureTableConnectionSettings) settings;
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
        if (state.state() != EConnectionState.Initialized) {
            throw new ConnectionError(String.format("[%s] Not initialized.", name()));
        }
        KeyStore keyStore = connectionManager().keyStore();
        Preconditions.checkNotNull(keyStore);
        try {
            synchronized (state) {
                String cs = keyStore.read(settings.getConnectionString());
                client = new TableServiceClientBuilder()
                        .connectionString(cs)
                        .buildClient();
                state.state(EConnectionState.Connected);
                return this;
            }
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
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
        return settings.getType();
    }

    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (client != null) {
                client = null;
            }
            if (state.isConnected()) {
                state.state(EConnectionState.Closed);
            }
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class AzureTableConnectionConfig extends ConnectionConfig {
        public static final String __CONFIG_PATH = "azure.table";

        private AzureTableConnectionSettings settings;

        public AzureTableConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
            settings = new AzureTableConnectionSettings();
        }

        public AzureTableConnectionSettings read() throws ConfigurationException {
            try {
                settings.setName(get().getString(ConnectionConfig.CONFIG_NAME));
                checkStringValue(settings.getName(), getClass(), ConnectionConfig.CONFIG_NAME);
                settings.setDb(get().getString(AzureTableConnectionSettings.Constants.CONFIG_DB_NAME));
                settings.setConnectionString(get().getString(AzureTableConnectionSettings.Constants.CONFIG_CONNECTION_STRING));
                settings.validate();

                return settings;
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
