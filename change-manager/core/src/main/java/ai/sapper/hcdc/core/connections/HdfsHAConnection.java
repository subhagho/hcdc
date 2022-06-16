package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class HdfsHAConnection extends HdfsConnection {
    private static class Constants {
        private static final String DFS_NAME_SERVICES = "dfs.nameservices";
        private static final String DFS_FAILOVER_PROVIDER = "dfs.client.failover.proxy.provider.%s";
        private static final String DFS_NAME_NODES = "dfs.ha.namenodes.%s";
        private static final String DFS_NAME_NODE_ADDRESS = "dfs.namenode.rpc-address.%s.%s";
    }

    private HdfsHAConfig config;

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
                config = new HdfsHAConfig(xmlConfig);
                config.read();

                hdfsConfig = new Configuration();
                hdfsConfig.set(Constants.DFS_NAME_SERVICES, config.nameService);
                hdfsConfig.set(String.format(Constants.DFS_FAILOVER_PROVIDER, config.nameService), config.failoverProvider);
                String nns = String.format("%s,%s", config.nameNodeAddresses[0][0], config.nameNodeAddresses[1][0]);
                hdfsConfig.set(String.format(Constants.DFS_NAME_NODES, config.nameService), nns);
                for (String[] nn : config.nameNodeAddresses) {
                    hdfsConfig.set(String.format(Constants.DFS_NAME_NODE_ADDRESS, config.nameService, nn[0]), nn[1]);
                }
                if (config.isSecurityEnabled) {
                    enableSecurity(hdfsConfig);
                }
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
    public HierarchicalConfiguration<ImmutableNode> config() {
        return config.get();
    }

    @Getter
    @Accessors(fluent = true)
    public static final class HdfsHAConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "hdfs_ha";

        private static class Constants {
            private static final String CONN_NAME = "name";
            private static final String DFS_NAME_SERVICES = "nameservice";
            private static final String DFS_FAILOVER_PROVIDER = "failoverProvider";
            private static final String DFS_NAME_NODES = "namenodes";

            private static final String CONN_SECURITY_ENABLED = "security.enabled";
            private static final String CONN_ADMIN_CLIENT_ENABLED = "enableAdmin";
        }

        private String name;
        private String nameService;
        private String failoverProvider;
        private String[][] nameNodeAddresses;
        private Map<String, String> parameters;
        private boolean isSecurityEnabled = false;
        private boolean isAdminEnabled = false;

        public HdfsHAConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not drt or is NULL");
            }
            try {
                name = get().getString(Constants.CONN_NAME);
                if (Strings.isNullOrEmpty(name)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.CONN_NAME));
                }
                nameService = get().getString(Constants.DFS_NAME_SERVICES);
                if (Strings.isNullOrEmpty(nameService)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.DFS_NAME_SERVICES));
                }
                failoverProvider = get().getString(Constants.DFS_FAILOVER_PROVIDER);
                if (Strings.isNullOrEmpty(failoverProvider)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.DFS_FAILOVER_PROVIDER));
                }
                String nn = get().getString(Constants.DFS_NAME_NODES);
                if (Strings.isNullOrEmpty(nn)) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.DFS_NAME_NODES));
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
                if (checkIfNodeExists((String) null, Constants.CONN_SECURITY_ENABLED))
                    isSecurityEnabled = get().getBoolean(Constants.CONN_SECURITY_ENABLED);
                if (checkIfNodeExists((String) null, Constants.CONN_ADMIN_CLIENT_ENABLED))
                    isAdminEnabled = get().getBoolean(Constants.CONN_ADMIN_CLIENT_ENABLED);

                parameters = readParameters();
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }
}
