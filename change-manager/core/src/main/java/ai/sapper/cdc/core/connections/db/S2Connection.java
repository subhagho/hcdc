package ai.sapper.cdc.core.connections.db;

import ai.sapper.cdc.core.keystore.KeyStore;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.settngs.JdbcConnectionSettings;
import com.google.common.base.Preconditions;
import com.singlestore.jdbc.SingleStorePoolDataSource;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.sql.SQLException;

@Getter
@Accessors(fluent = true)
public class S2Connection extends DbConnection {
    private SingleStorePoolDataSource poolDataSource;
    private S2ConnectionConfig config;

    public S2Connection() {
        super(S2ConnectionConfig.__CONFIG_PATH);
    }

    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull ConnectionManager connectionManager) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                this.connectionManager = connectionManager;
                config = new S2ConnectionConfig(xmlConfig);
                settings = config.read();

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
                String jdbc = createJdbcUrl(keyStore);
                poolDataSource = new SingleStorePoolDataSource(jdbc);

                state.state(EConnectionState.Connected);
                return this;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    public java.sql.Connection getConnection() throws SQLException {
        Preconditions.checkState(isConnected());
        return poolDataSource.getConnection();
    }

    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.state(EConnectionState.Closed);
            }
            if (poolDataSource != null) {
                poolDataSource.close();
                poolDataSource = null;
            }
        }
    }


    @Override
    public String path() {
        return S2ConnectionConfig.__CONFIG_PATH;
    }


    @Getter
    @Accessors(fluent = true)
    public static class S2ConnectionConfig extends DbConnectionConfig {
        public static final String __CONFIG_PATH = "s2";

        private JdbcConnectionSettings settings;

        public S2ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public JdbcConnectionSettings read() throws ConfigurationException {
            settings = super.read();
            settings.setConnectionType(S2Connection.class);
            return settings;
        }
    }
}
