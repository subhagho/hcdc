package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.DefaultLogger;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class HdfsHAConnection implements Connection {
    private static class Constants {
        private static final String DFS_NAME_SERVICES = "dfs.nameservices";
        private static final String DFS_FAILOVER_PROVIDER = "dfs.client.failover.proxy.provider.%s";
        private static final String DFS_NAME_NODES = "dfs.ha.namenodes.%s";
        private static final String DFS_NAME_NODE_ADDRESS = "dfs.namenode.rpc-address.%s.%s";
    }

    @Getter(AccessLevel.NONE)
    private final ConnectionState state = new ConnectionState();

    private HdfsHAConfig config;

    private Configuration hdfsConfig = null;
    private FileSystem fileSystem;
    private HdfsAdmin adminClient;

    /**
     * @return
     */
    @Override
    public String name() {
        return null;
    }

    /**
     * @param xmlConfig
     * @param pathPrefix
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
                config = new HdfsHAConfig(xmlConfig, pathPrefix);
                config.read();

                hdfsConfig = new Configuration();
                hdfsConfig.set(Constants.DFS_NAME_SERVICES, config.nameService);
                hdfsConfig.set(String.format(Constants.DFS_FAILOVER_PROVIDER, config.nameService), config.failoverProvider);
                String nns = String.format("%s,%s", config.nameNodeAddresses[0][0], config.nameNodeAddresses[1][0]);
                hdfsConfig.set(String.format(Constants.DFS_NAME_NODES, config.nameService), nns);
                for (String[] nn : config.nameNodeAddresses) {
                    hdfsConfig.set(String.format(Constants.DFS_NAME_NODE_ADDRESS, config.nameService, nn[0]), nn[1]);
                }
                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }


    private void enableSecurity(Configuration conf) throws Exception {
        HdfsConnection.HdfsSecurityConfig sConfig = new HdfsConnection.HdfsSecurityConfig(config.config(), config.path());
        sConfig.read();
        sConfig.setup(conf);
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        synchronized (state) {
            if (!state.isConnected() && state.state() == EConnectionState.Initialized) {
                state.clear(EConnectionState.Initialized);
                try {
                    fileSystem = FileSystem.get(URI.create(String.format("hdfs://%s", config.nameService)), hdfsConfig);
                    if (config.isAdminEnabled) {
                        adminClient = new HdfsAdmin(URI.create(String.format("hdfs://%s", config.nameService)), hdfsConfig);
                    }
                    if (config.parameters != null && !config.parameters.isEmpty()) {
                        for (String key : config.parameters.keySet()) {
                            hdfsConfig.set(key, config.parameters.get(key));
                        }
                    }
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
        return config.get();
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public EConnectionState close() throws ConnectionError {
        synchronized (state) {
            if (state.isConnected()) {
                state.state(EConnectionState.Closed);
            }
            try {
                if (fileSystem != null) {
                    fileSystem.close();
                    fileSystem = null;
                }
                if (adminClient != null) {
                    adminClient = null;
                }
            } catch (Exception ex) {
                state.error(ex);
                throw new ConnectionError("Error closing HDFS connection.", ex);
            }
            return state.state();
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static final class HdfsHAConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "connection.hdfs_ha";

        private static class Constants {
            private static final String CONN_NAME = "name";
            private static final String DFS_NAME_SERVICES = "nameservice";
            private static final String DFS_FAILOVER_PROVIDER = "failover_provider";
            private static final String DFS_NAME_NODES = "namenodes";

            private static final String CONN_SECURITY_ENABLED = "security.enabled";
            private static final String CONN_ADMIN_CLIENT_ENABLED = "enable_admin";
        }

        private HierarchicalConfiguration<ImmutableNode> node;
        private String name;
        private String nameService;
        private String failoverProvider;
        private String[][] nameNodeAddresses;
        private Map<String, String> parameters;
        private boolean isSecurityEnabled = false;
        private boolean isAdminEnabled = false;

        public HdfsHAConfig(@NonNull XMLConfiguration config, String pathPrefix) {
            super(config, __CONFIG_PATH, pathPrefix);
        }

        public void read() throws ConfigurationException {
            node = get();
            if (node == null) {
                throw new ConfigurationException(String.format("HDFS Configuration not found. [path=%s]", path()));
            }
            try {
                name = node.getString(Constants.CONN_NAME);
                if (Strings.isNullOrEmpty(name)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s.%s]", path(), Constants.CONN_NAME));
                }
                nameService = node.getString(Constants.DFS_NAME_SERVICES);
                if (Strings.isNullOrEmpty(nameService)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s.%s]", path(), Constants.DFS_NAME_SERVICES));
                }
                failoverProvider = node.getString(Constants.DFS_FAILOVER_PROVIDER);
                if (Strings.isNullOrEmpty(failoverProvider)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s.%s]", path(), Constants.DFS_FAILOVER_PROVIDER));
                }
                String nn = node.getString(Constants.DFS_NAME_NODES);
                if (Strings.isNullOrEmpty(nn)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s.%s]", path(), Constants.DFS_NAME_NODES));
                }
                String[] nns = nn.split(";");
                if (nns.length != 2) {
                    throw new ConfigurationException(String.format("Invalid NameNode(s) specified. Expected count = 2, specified = %d", nns.length));
                }
                nameNodeAddresses = new String[2][2];

                for (int ii = 0; ii < nns.length; ii++) {
                    String n = nns[ii];
                    String[] parts = n.split("=");
                    if (parts.length != 2) {
                        throw new ConfigurationException(String.format("Invalid NameNode specified. Expected count = 2, specified = %d", parts.length));
                    }
                    String key = parts[0].trim();
                    String address = parts[1].trim();

                    DefaultLogger.__LOG.info(String.format("Registering namenode [%s -> %s]...", key, address));
                    nameNodeAddresses[ii][0] = key;
                    nameNodeAddresses[ii][1] = address;
                }
                isSecurityEnabled = node.getBoolean(Constants.CONN_SECURITY_ENABLED);
                isAdminEnabled = node.getBoolean(Constants.CONN_ADMIN_CLIENT_ENABLED);

                parameters = readParameters(node);
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }
}
