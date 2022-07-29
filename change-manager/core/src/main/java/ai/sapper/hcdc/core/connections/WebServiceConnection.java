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

import java.io.IOException;
import java.net.URL;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class WebServiceConnection implements Connection {
    @Getter(AccessLevel.NONE)
    protected final ConnectionState state = new ConnectionState();
    private URL endpoint;
    private Map<String, String> paths;
    private WebServiceConnectionConfig config;

    /**
     * @return
     */
    @Override
    public String name() {
        return config.name();
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        return null;
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
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
        return false;
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
        private static final String __CONFIG_PATH = "service.web";

        public static class Constants {
            public static final String CONFIG_NAME = "name";
            public static final String CONFIG_URL = "endpoint";
            public static final String CONFIG_PATH_MAP = "paths";
            public static final String CONFIG_RETRIES = "retries";
        }

        private String name;
        private String endpoint;
        private Map<String, String> pathMap;
        private int retries = 0;

        public WebServiceConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("WebService client Configuration not set or is NULL");
            }
            name = get().getString(Constants.CONFIG_NAME);
            if (Strings.isNullOrEmpty(name)) {
                throw new ConfigurationException(String.format("WebService client Configuration Error: missing [%s]",
                        Constants.CONFIG_NAME));
            }
            endpoint = get().getString(Constants.CONFIG_URL);
            if (Strings.isNullOrEmpty(endpoint)) {
                throw new ConfigurationException(String.format("WebService client Configuration Error: missing [%s]",
                        Constants.CONFIG_URL));
            }
            String s = get().getString(Constants.CONFIG_RETRIES);
            if (!Strings.isNullOrEmpty(s)) {
                retries = Integer.parseInt(s);
            }
        }
    }
}
