package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.EConnectionType;
import ai.sapper.cdc.core.connections.settngs.ZookeeperSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class ZookeeperConnection implements Connection {
    @Getter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();
    private CuratorFramework client;
    private CuratorFrameworkFactory.Builder builder;
    private ZookeeperConfig config;
    private ZookeeperSettings settings;

    /**
     * @return
     */
    @Override
    public String name() {
        return settings.getName();
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull BaseEnv<?> env) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);

                config = new ZookeeperConfig(xmlConfig);
                settings = config.read();

                setup();

                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    private void setup() throws Exception {
        builder = CuratorFrameworkFactory.builder();
        builder.connectString(settings.getConnectionString());
        if (settings.isRetryEnabled()) {
            builder.retryPolicy(new ExponentialBackoffRetry(settings.getRetryInterval(), settings.getRetryCount()));
        }
        if (!Strings.isNullOrEmpty(settings.getAuthenticationHandler())) {
            Class<? extends ZookeeperAuthHandler> cls
                    = (Class<? extends ZookeeperAuthHandler>) Class.forName(settings.getAuthenticationHandler());
            ZookeeperAuthHandler authHandler = cls.newInstance();
            authHandler.setup(builder, config.config());
        }
        if (!Strings.isNullOrEmpty(settings.getNamespace())) {
            builder.namespace(settings.getNamespace());
        }
        if (settings.getConnectionTimeout() > 0) {
            builder.connectionTimeoutMs(settings.getConnectionTimeout());
        }
        if (settings.getSessionTimeout() > 0) {
            builder.sessionTimeoutMs(settings.getSessionTimeout());
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
                        .withPath(ZookeeperConfig.__CONFIG_PATH)
                        .build();
                if (client.checkExists().forPath(hpath) == null) {
                    throw new Exception(String.format("HDFS Settings path not found. [path=%s]", hpath));
                }
                byte[] data = client.getData().forPath(hpath);
                settings = JSONUtils.read(data, ZookeeperSettings.class);
                Preconditions.checkNotNull(settings);
                Preconditions.checkState(name.equals(settings.getName()));

                setup();

                state.state(EConnectionState.Initialized);
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull BaseEnv<?> env) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof ZookeeperSettings);
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                this.settings = (ZookeeperSettings) settings;
                setup();

                state.state(EConnectionState.Initialized);
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        synchronized (state) {
            if (!state.isConnected()
                    && (state.state() == EConnectionState.Initialized || state.state() == EConnectionState.Closed)) {
                state.clear(EConnectionState.Initialized);
                try {
                    client = builder.build();
                    client.start();
                    client.blockUntilConnected();

                    state.state(EConnectionState.Connected);
                } catch (Throwable t) {
                    state.error(t);
                    throw new ConnectionError("Error opening HDFS connection.", t);
                }
            }
        }
        return this;
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return state.error();
    }

    /**
     * @return
     */
    @Override
    public EConnectionState connectionState() {
        return state.state();
    }

    /**
     * @return
     */
    @Override
    public boolean isConnected() {
        return state.isConnected();
    }

    @Override
    public String path() {
        return ZookeeperConfig.__CONFIG_PATH;
    }

    @Override
    public EConnectionType type() {
        return settings.getType();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.state(EConnectionState.Closed);
            }
            try {
                if (client != null) {
                    client.close();
                    client = null;
                }
            } catch (Exception ex) {
                state.error(ex);
                throw new IOException("Error closing HDFS connection.", ex);
            }
        }
    }


    public static class ZookeeperConfig extends ConnectionConfig {
        public static final class Constants {
            public static final String CONFIG_CONNECTION = "connectionString";
            public static final String CONFIG_AUTH_HANDLER = "authenticationHandler";
            public static final String CONFIG_RETRY = "retry";
            public static final String CONFIG_RETRY_INTERVAL = "retry.interval";
            public static final String CONFIG_RETRY_TRIES = "retry.retries";
            public static final String CONFIG_CONN_TIMEOUT = "connectionTimeout";
            public static final String CONFIG_SESSION_TIMEOUT = "sessionTimeout";
            public static final String CONFIG_NAMESPACE = "namespace";
            public static final String CONFIG_ZK_CONFIG = "zookeeperConfigFile";
        }

        private static final String __CONFIG_PATH = "zookeeper";

        private final ZookeeperSettings settings = new ZookeeperSettings();

        public ZookeeperSettings read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not drt or is NULL");
            }
            try {
                settings.setName(get().getString(CONFIG_NAME));
                checkStringValue(settings.getName(), getClass(), CONFIG_NAME);
                settings.setConnectionString(get().getString(Constants.CONFIG_CONNECTION));
                checkStringValue(settings.getConnectionString(), getClass(), Constants.CONFIG_CONNECTION);
                if (checkIfNodeExists((String) null, Constants.CONFIG_AUTH_HANDLER)) {
                    settings.setAuthenticationHandler(get().getString(Constants.CONFIG_AUTH_HANDLER));
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_NAMESPACE)) {
                    settings.setNamespace(get().getString(Constants.CONFIG_NAMESPACE));
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_RETRY)) {
                    settings.setRetryEnabled(true);
                    if (checkIfNodeExists((String) null, Constants.CONFIG_RETRY_INTERVAL)) {
                        settings.setRetryInterval(get().getInt(Constants.CONFIG_RETRY_INTERVAL));
                    }
                    if (checkIfNodeExists((String) null, Constants.CONFIG_RETRY_TRIES)) {
                        settings.setRetryCount(get().getInt(Constants.CONFIG_RETRY_TRIES));
                    }
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_CONN_TIMEOUT)) {
                    settings.setConnectionTimeout(get().getInt(Constants.CONFIG_CONN_TIMEOUT));
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_SESSION_TIMEOUT)) {
                    settings.setSessionTimeout(get().getInt(Constants.CONFIG_SESSION_TIMEOUT));
                }
                return settings;
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }

        public ZookeeperConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
