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
                StringBuilder snn = new StringBuilder();
                for (String nn : config.nameNodeAddresses.keySet()) {

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
        return null;
    }

    /**
     * @return
     */
    @Override
    public Throwable error() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public EConnectionState state() {
        return null;
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
    public static final class HdfsHAConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "connection.hdfs_ha";

        private static class Constants {
            private static final String CONN_NAME = "name";
            private static final String DFS_NAME_SERVICES = "nameservice";
            private static final String DFS_FAILOVER_PROVIDER = "failover_provider";
            private static final String DFS_NAME_NODES = "namenodes";
        }

        private HierarchicalConfiguration<ImmutableNode> node;
        private String name;
        private String nameService;
        private String failoverProvider;
        private Map<String, String> nameNodeAddresses;
        private Map<String, String> parameters;

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
                for (String n : nns) {
                    nameNodeAddresses = new HashMap<>(2);
                    String[] parts = n.split("=");
                    if (parts.length != 2) {
                        throw new ConfigurationException(String.format("Invalid NameNode specified. Expected count = 2, specified = %d", parts.length));
                    }
                    String key = parts[0].trim();
                    String address = parts[1].trim();

                    DefaultLogger.__LOG.info(String.format("Registering namenode [%s -> %s]...", key, address));
                    nameNodeAddresses.put(key, address);
                }
                parameters = readParameters(node);
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }
}
