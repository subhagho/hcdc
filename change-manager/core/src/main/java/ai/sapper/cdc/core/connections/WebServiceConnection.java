package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.ConfigReader;
import com.google.common.base.Strings;
import com.sun.jersey.api.client.filter.LoggingFilter;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.WebTarget;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.JerseyClientBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

@Getter
@Accessors(fluent = true)
public class WebServiceConnection implements Connection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    private URL endpoint;
    private WebServiceConnectionConfig config;
    private Client client;
    private String name;

    public WebServiceConnection() {
    }

    public WebServiceConnection(@NonNull String name,
                                @NonNull String endpoint) throws MalformedURLException {
        this.name = name;
        this.endpoint = new URL(endpoint);
        client = JerseyClientBuilder.newClient(
                new ClientConfig().register(LoggingFilter.class));
        state.state(EConnectionState.Initialized);
    }

    /**
     * @return
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        try {
            config = new WebServiceConnectionConfig(xmlConfig);
            config.read();

            name = config.name;
            endpoint = new URL(config.endpoint);
            client = JerseyClientBuilder.newClient(
                    new ClientConfig().register(LoggingFilter.class));
            state.state(EConnectionState.Initialized);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            throw new ConnectionError(ex);
        }
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        throw new ConnectionError("Method should not be called...");
    }

    public WebTarget connect(@NonNull String path) throws ConnectionError {
        try {
            return client.target(endpoint.toURI()).path(path);
        } catch (Exception ex) {
            throw new ConnectionError(ex);
        }
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
        return state.state() == EConnectionState.Initialized;
    }

    /**
     * @return
     */
    @Override
    public HierarchicalConfiguration<ImmutableNode> config() {
        return config.config();
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {

    }

    @Getter
    @Accessors(fluent = true)
    public static class WebServiceConnectionConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "rest";

        public static class Constants {
            public static final String CONFIG_NAME = "name";
            public static final String CONFIG_URL = "endpoint";
        }

        private String name;
        private String endpoint;

        public WebServiceConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("WebService connection Configuration not set or is NULL");
            }
            name = get().getString(Constants.CONFIG_NAME);
            if (Strings.isNullOrEmpty(name)) {
                throw new ConfigurationException(String.format("WebService connection Configuration Error: missing [%s]",
                        Constants.CONFIG_NAME));
            }
            endpoint = get().getString(Constants.CONFIG_URL);
            if (Strings.isNullOrEmpty(endpoint)) {
                throw new ConfigurationException(String.format("WebService connection Configuration Error: missing [%s]",
                        Constants.CONFIG_URL));
            }
        }
    }
}
