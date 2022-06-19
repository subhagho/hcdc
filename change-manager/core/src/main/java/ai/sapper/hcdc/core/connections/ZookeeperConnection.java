package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
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
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class ZookeeperConnection implements Connection {
    @Getter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();
    private CuratorFramework client;
    private CuratorFrameworkFactory.Builder builder;
    private ZookeeperConfig config;

    /**
     * @return
     */
    @Override
    public String name() {
        return config.name;
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);

                config = new ZookeeperConfig(xmlConfig);
                config.read();

                builder = CuratorFrameworkFactory.builder();
                builder.connectString(config.connectionString);
                if (config.isRetryEnabled) {
                    builder.retryPolicy(new ExponentialBackoffRetry(config.retryInterval, config.retryCount));
                }
                if (!Strings.isNullOrEmpty(config.authenticationHandler)) {
                    Class<? extends ZookeeperAuthHandler> cls
                            = (Class<? extends ZookeeperAuthHandler>) Class.forName(config.authenticationHandler);
                    ZookeeperAuthHandler authHandler = cls.newInstance();
                    authHandler.setup(builder, config.config());
                }
                if (!Strings.isNullOrEmpty(config.namespace)) {
                    builder.namespace(config.namespace);
                }
                if (config.connectionTimeout > 0) {
                    builder.connectionTimeoutMs(config.connectionTimeout);
                }
                if (config.sessionTimeout > 0) {
                    builder.sessionTimeoutMs(config.sessionTimeout);
                }

                /*
                if (!Strings.isNullOrEmpty(config.zookeeperConfig)) {
                    File zkf = new File(config.zookeeperConfig);
                    if (!zkf.exists()) {
                        throw new ConnectionError(String.format("Specified ZooKeeper configuration file not found. [path=%s]", zkf.getAbsolutePath()));
                    }
                    builder.zkClientConfig(new ZKClientConfig(zkf));
                }
                */

                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
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

    /**
     * @return
     */
    @Override
    public HierarchicalConfiguration<ImmutableNode> config() {
        return config.config();
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

    public static class ZookeeperConfig extends ConfigReader {
        private static final class Constants {
            private static final String CONFIG_NAME = "name";
            private static final String CONFIG_CONNECTION = "connectionString";
            private static final String CONFIG_AUTH_HANDLER = "authenticationHandler";
            private static final String CONFIG_RETRY = "retry";
            private static final String CONFIG_RETRY_INTERVAL = "retry.interval";
            private static final String CONFIG_RETRY_TRIES = "retry.retries";
            private static final String CONFIG_CONN_TIMEOUT = "connectionTimeout";
            private static final String CONFIG_SESSION_TIMEOUT = "sessionTimeout";
            private static final String CONFIG_NAMESPACE = "namespace";
            private static final String CONFIG_ZK_CONFIG = "zookeeperConfigFile";
        }

        private static final String __CONFIG_PATH = "zookeeper";
        private String name;
        private String connectionString;
        private String authenticationHandler;
        private String namespace;
        private boolean isRetryEnabled = false;
        private int retryInterval = 1000;
        private int retryCount = 3;
        private int connectionTimeout = -1;
        private int sessionTimeout = -1;
        private String zookeeperConfig;

        private Map<String, String> parameters;

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not drt or is NULL");
            }
            try {
                name = get().getString(Constants.CONFIG_NAME);
                if (Strings.isNullOrEmpty(name)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.CONFIG_NAME));
                }
                connectionString = get().getString(Constants.CONFIG_CONNECTION);
                if (Strings.isNullOrEmpty(connectionString)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION));
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_AUTH_HANDLER)) {
                    authenticationHandler = get().getString(Constants.CONFIG_AUTH_HANDLER);
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_NAMESPACE)) {
                    namespace = get().getString(Constants.CONFIG_NAMESPACE);
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_RETRY)) {
                    isRetryEnabled = true;
                    if (checkIfNodeExists((String) null, Constants.CONFIG_RETRY_INTERVAL)) {
                        retryInterval = get().getInt(Constants.CONFIG_RETRY_INTERVAL);
                    }
                    if (checkIfNodeExists((String) null, Constants.CONFIG_RETRY_TRIES)) {
                        retryCount = get().getInt(Constants.CONFIG_RETRY_TRIES);
                    }
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_CONN_TIMEOUT)) {
                    connectionTimeout = get().getInt(Constants.CONFIG_CONN_TIMEOUT);
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_SESSION_TIMEOUT)) {
                    sessionTimeout = get().getInt(Constants.CONFIG_SESSION_TIMEOUT);
                }
                if (checkIfNodeExists((String) null, Constants.CONFIG_ZK_CONFIG)) {
                    zookeeperConfig = get().getString(Constants.CONFIG_ZK_CONFIG);
                }
                parameters = readParameters();
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }

        public ZookeeperConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
