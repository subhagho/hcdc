package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class HdfsConnection implements Connection {

    private final ConnectionState state = new ConnectionState();
    private HdfsConfig config;

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull XMLConfiguration xmlConfig, String pathPrefix) throws ConnectionError {
        synchronized (state) {
            if (state.isConnected()) {
                close();
            }
            state.clear(EConnectionState.Unknown);
            try {
                config = new HdfsConfig(xmlConfig, pathPrefix);

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
            if (!state.isConnected() && !state.hasError()) {
                state.clear(EConnectionState.Initialized);
                try {

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
    public EConnectionState state() {
        return state.state();
    }

    /**
     * @return
     */
    @Override
    public HierarchicalConfiguration<ImmutableNode> config() {
        return null;
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public EConnectionState close() throws ConnectionError {
        return null;
    }

    @Getter
    @Accessors(fluent = true)
    public static class HdfsConfig extends ConfigReader {
        private static final class Constants {
            private static final String CONN_PRI_NAME_NODE_URI = "PRIMARY_NAME_NODE_URI";
            private static final String CONN_SEC_NAME_NODE_URI = "SECONDARY_NAME_NODE_URI";
            private static final String CONN_SECURITY_ENABLED = "SECURITY_ENABLED";
        }

        private static final String __CONFIG_PATH = "connection.hdfs";

        private HierarchicalConfiguration<ImmutableNode> config;
        private String primaryNameNodeUri;
        private String secondaryNameNodeUri;
        private boolean isSecurityEnabled = false;

        public HdfsConfig(@NonNull XMLConfiguration config, String pathPrefix) {
            super(config, __CONFIG_PATH, pathPrefix);
        }

        public void read() throws ConfigurationException {
            config = get();
            if (config == null) {
                throw new ConfigurationException(String.format("HDFS Configuration not found. [path=%s]", path()));
            }
            try {
                primaryNameNodeUri = config.getString(Constants.CONN_PRI_NAME_NODE_URI);
                if (Strings.isNullOrEmpty(primaryNameNodeUri)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s.%s]", path(), Constants.CONN_PRI_NAME_NODE_URI));
                }
                secondaryNameNodeUri = config.getString(Constants.CONN_SEC_NAME_NODE_URI);
                if (Strings.isNullOrEmpty(secondaryNameNodeUri)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s.%s]", path(), Constants.CONN_SEC_NAME_NODE_URI));
                }
                isSecurityEnabled = config.getBoolean(Constants.CONN_SECURITY_ENABLED);
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }
}
