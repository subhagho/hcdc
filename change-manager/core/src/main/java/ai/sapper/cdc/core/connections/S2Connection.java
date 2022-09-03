package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.KeyStore;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.HdfsConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.S2ConnectionSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.singlestore.jdbc.SingleStorePoolDataSource;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.sql.SQLException;

@Getter
@Accessors(fluent = true)
public class S2Connection implements Connection {
    public static class Constants {
        public static final String S2_KEY_USER = "user=";
        public static final String S2_KEY_PASSWD = "password=";
        public static final String S2_KEY_POOL_SIZE = "maxPoolSize=";
    }

    @Getter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();
    private S2ConnectionConfig config;
    private SingleStorePoolDataSource poolDataSource;
    private S2ConnectionSettings settings;
    private ConnectionManager connectionManager;

    @Override
    public String name() {
        return settings.getName();
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
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull ConnectionManager connectionManager) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                CuratorFramework client = connection.client();
                String hpath = new PathUtils.ZkPathBuilder(path)
                        .withPath(S2ConnectionConfig.__CONFIG_PATH)
                        .build();
                if (client.checkExists().forPath(hpath) == null) {
                    throw new Exception(String.format("HDFS Settings path not found. [path=%s]", hpath));
                }
                byte[] data = client.getData().forPath(hpath);
                settings = JSONUtils.read(data, S2ConnectionSettings.class);
                Preconditions.checkNotNull(settings);
                Preconditions.checkState(name.equals(settings.getName()));
                this.connectionManager = connectionManager;
                state.state(EConnectionState.Initialized);
                return this;
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull ConnectionManager connectionManager) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                this.settings = (S2ConnectionSettings) settings;
                this.connectionManager = connectionManager;
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

    private String createJdbcUrl(KeyStore keyStore) throws Exception {
        String url = settings.getJdbcUrl().trim();
        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 2);
        }
        StringBuilder builder = new StringBuilder(settings.getJdbcUrl());
        if (!Strings.isNullOrEmpty(settings.getDb())) {
            builder.append("/")
                    .append(settings.getDb());
        }
        builder.append("?")
                .append(Constants.S2_KEY_USER)
                .append(settings.getUser());
        String pk = settings.getPassword();
        builder.append("&")
                .append(Constants.S2_KEY_PASSWD)
                .append(keyStore.read(pk));
        builder.append("&")
                .append(Constants.S2_KEY_POOL_SIZE)
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
        return S2ConnectionConfig.__CONFIG_PATH;
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

    @Getter
    @Accessors(fluent = true)
    public static class S2ConnectionConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "s2";

        public static class Constants {
            public static final String CONFIG_NAME = "name";
            public static final String CONFIG_JDBC_URL = "jdbcUrl";
            public static final String CONFIG_USER = "user";
            public static final String CONFIG_PASS_KEY = "passwordKay";
            public static final String CONFIG_POOL_SIZE = "poolSize";
            public static final String CONFIG_DB_NAME = "db";
        }

        private final S2ConnectionSettings settings = new S2ConnectionSettings();

        public S2ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public S2ConnectionSettings read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("SingleStore Configuration not drt or is NULL");
            }
            try {
                settings.setName(get().getString(Constants.CONFIG_NAME));
                if (Strings.isNullOrEmpty(settings.getName())) {
                    throw new ConfigurationException(
                            String.format("SingleStore Configuration Error: missing [%s]", Constants.CONFIG_NAME));
                }
                settings.setJdbcUrl(get().getString(Constants.CONFIG_JDBC_URL));
                if (Strings.isNullOrEmpty(settings.getJdbcUrl())) {
                    throw new ConfigurationException(
                            String.format("SingleStore Configuration Error: missing [%s]", Constants.CONFIG_JDBC_URL));
                }
                settings.setUser(get().getString(Constants.CONFIG_USER));
                if (Strings.isNullOrEmpty(settings.getUser())) {
                    throw new ConfigurationException(
                            String.format("SingleStore Configuration Error: missing [%s]", Constants.CONFIG_USER));
                }
                settings.setPassword(get().getString(Constants.CONFIG_PASS_KEY));
                if (Strings.isNullOrEmpty(settings.getPassword())) {
                    throw new ConfigurationException(
                            String.format("SingleStore Configuration Error: missing [%s]", Constants.CONFIG_PASS_KEY));
                }
                String s = get().getString(Constants.CONFIG_POOL_SIZE);
                if (!Strings.isNullOrEmpty(s)) {
                    settings.setPoolSize(Integer.parseInt(s));
                }
                s = get().getString(Constants.CONFIG_DB_NAME);
                if (!Strings.isNullOrEmpty(s)) {
                    settings.setDb(s);
                }
                settings.setParameters(readParameters());

                return settings;
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
