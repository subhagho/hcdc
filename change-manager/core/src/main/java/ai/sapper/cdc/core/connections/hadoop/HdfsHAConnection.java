package ai.sapper.cdc.core.connections.hadoop;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.cdc.core.connections.settngs.HdfsConnectionSettings;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.net.URI;

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
        return settings.getName();
    }

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull ConnectionManager connectionManager) throws ConnectionError {
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                config = new HdfsHAConfig(xmlConfig);
                settings = config.read();

                setupHadoopConfig();

                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull ConnectionManager connectionManager) throws ConnectionError {
        Preconditions.checkArgument(settings instanceof HdfsConnectionSettings.HdfsHASettings);
        synchronized (state) {
            try {
                if (state.isConnected()) {
                    close();
                }
                state.clear(EConnectionState.Unknown);
                this.settings = (HdfsConnectionSettings.HdfsBaseSettings) settings;

                setupHadoopConfig();

                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
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
                        .withPath(HdfsHAConfig.__CONFIG_PATH)
                        .build();
                if (client.checkExists().forPath(hpath) == null) {
                    throw new Exception(String.format("HDFS Settings path not found. [path=%s]", hpath));
                }
                byte[] data = client.getData().forPath(hpath);
                settings = JSONUtils.read(data, HdfsConnectionSettings.HdfsHASettings.class);
                Preconditions.checkNotNull(settings);
                Preconditions.checkState(name.equals(settings.getName()));

                setupHadoopConfig();

                state.state(EConnectionState.Initialized);
            } catch (Exception ex) {
                throw new ConnectionError(ex);
            }
        }
        return this;
    }

    private void setupHadoopConfig() throws Exception {
        Preconditions.checkState(settings instanceof HdfsConnectionSettings.HdfsHASettings);
        hdfsConfig = new Configuration();
        hdfsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getCanonicalName());
        hdfsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getCanonicalName());
        hdfsConfig.set(Constants.DFS_NAME_SERVICES, ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService());
        hdfsConfig.set(String.format(Constants.DFS_FAILOVER_PROVIDER,
                        ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService()),
                ((HdfsConnectionSettings.HdfsHASettings) settings).getFailoverProvider());
        String nns = String.format("%s,%s",
                ((HdfsConnectionSettings.HdfsHASettings) settings).getNameNodeAddresses()[0][0],
                ((HdfsConnectionSettings.HdfsHASettings) settings).getNameNodeAddresses()[1][0]);
        hdfsConfig.set(String.format(Constants.DFS_NAME_NODES,
                ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService()), nns);
        for (String[] nn : ((HdfsConnectionSettings.HdfsHASettings) settings).getNameNodeAddresses()) {
            hdfsConfig.set(String.format(Constants.DFS_NAME_NODE_ADDRESS,
                    ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService(), nn[0]), nn[1]);
        }
        if (settings.isSecurityEnabled()) {
            enableSecurity(hdfsConfig);
        }
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        Preconditions.checkState(settings instanceof HdfsConnectionSettings.HdfsHASettings);
        synchronized (state) {
            if (!state.isConnected()
                    && (state.state() == EConnectionState.Initialized || state.state() == EConnectionState.Closed)) {
                state.clear(EConnectionState.Initialized);
                try {
                    fileSystem = FileSystem.get(URI.create(String.format("hdfs://%s",
                            ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService())), hdfsConfig);
                    if (settings.isAdminEnabled()) {
                        adminClient = new HdfsAdmin(URI.create(String.format("hdfs://%s",
                                ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService())), hdfsConfig);
                    }
                    if (settings.getParameters() != null && !settings.getParameters().isEmpty()) {
                        for (String key : settings.getParameters().keySet()) {
                            hdfsConfig.set(key, settings.getParameters().get(key));
                        }
                    }
                    dfsClient = new DFSClient(URI.create(String.format("hdfs://%s",
                            ((HdfsConnectionSettings.HdfsHASettings) settings).getNameService())), hdfsConfig);

                    state.state(EConnectionState.Connected);
                } catch (Throwable t) {
                    state.error(t);
                    throw new ConnectionError("Error opening HDFS connection.", t);
                }
            }
        }
        return this;
    }

    @Override
    public String path() {
        return HdfsHAConfig.__CONFIG_PATH;
    }


    @Getter
    @Accessors(fluent = true)
    public static final class HdfsHAConfig extends HdfsConfig {
        private static final String __CONFIG_PATH = "hdfs_ha";

        private static class Constants {
            private static final String CONN_NAME = "name";
            private static final String DFS_NAME_SERVICES = "nameservice";
            private static final String DFS_FAILOVER_PROVIDER = "failoverProvider";
            private static final String DFS_NAME_NODES = "namenodes";

            private static final String CONN_SECURITY_ENABLED = "security.enabled";
            private static final String CONN_ADMIN_CLIENT_ENABLED = "enableAdmin";
        }

        public HdfsHAConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, new HdfsConnectionSettings.HdfsHASettings());
        }

        @Override
        public HdfsConnectionSettings.HdfsBaseSettings read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("HDFS Configuration not drt or is NULL");
            }
            try {
                HdfsConnectionSettings.HdfsHASettings settings = (HdfsConnectionSettings.HdfsHASettings) settings();
                settings.setName(get().getString(Constants.CONN_NAME));
                if (Strings.isNullOrEmpty(settings.getName())) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.CONN_NAME));
                }
                settings.setNameService(get().getString(Constants.DFS_NAME_SERVICES));
                if (Strings.isNullOrEmpty(settings.getNameService())) {
                    throw new ConfigurationException(String.format("HDFS Configuration Error: missing [%s]", Constants.DFS_NAME_SERVICES));
                }
                settings.setFailoverProvider(get().getString(Constants.DFS_FAILOVER_PROVIDER));
                if (Strings.isNullOrEmpty(settings.getFailoverProvider())) {
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
                settings.setNameNodeAddresses(new String[2][2]);

                for (int ii = 0; ii < nns.length; ii++) {
                    String n = nns[ii];
                    String[] parts = n.split("=");
                    if (parts.length != 2) {
                        throw new ConfigurationException(String.format("Invalid NameNode specified. Expected count = 2, specified = %d", parts.length));
                    }
                    String key = parts[0].trim();
                    String address = parts[1].trim();

                    DefaultLogger.LOGGER.info(String.format("Registering namenode [%s -> %s]...", key, address));
                    settings.getNameNodeAddresses()[ii][0] = key;
                    settings.getNameNodeAddresses()[ii][1] = address;
                }
                if (checkIfNodeExists((String) null, Constants.CONN_SECURITY_ENABLED))
                    settings.setSecurityEnabled(get().getBoolean(Constants.CONN_SECURITY_ENABLED));
                if (checkIfNodeExists((String) null, Constants.CONN_ADMIN_CLIENT_ENABLED))
                    settings.setAdminEnabled(get().getBoolean(Constants.CONN_ADMIN_CLIENT_ENABLED));

                settings.setParameters(readParameters());

                return settings;
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing HDFS configuration.", t);
            }
        }
    }
}
