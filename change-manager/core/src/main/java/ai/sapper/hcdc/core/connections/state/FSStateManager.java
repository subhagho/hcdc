package ai.sapper.hcdc.core.connections.state;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.nio.charset.Charset;

@Getter
@Accessors(fluent = true)
public class FSStateManager {
    private ZookeeperConnection connection;
    private FSStateManagerConfig config;

    public FSStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull ConnectionManager manger) throws StateManagerError {
        try {
            config = new FSStateManagerConfig(xmlConfig);
            config.read();

            connection = manger.getConnection(config.zkConnection, ZookeeperConnection.class);
            CuratorFramework client = connection().client();
            if (!connection.isConnected()) {
                throw new StateManagerError("Error initializing ZooKeeper connection.");
            }
            if (client.checkExists().forPath(basePath()) == null) {
                String path = client.create().creatingParentContainersIfNeeded().forPath(basePath());
                if (Strings.isNullOrEmpty(path)) {
                    throw new StateManagerError(String.format("Error creating ZK base path. [path=%s]", basePath()));
                }
            }
            return this;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public String basePath() {
        return config().basePath();
    }

    public DFSFileState createFileHandle(@NonNull String path) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        try {
            CuratorFramework client = connection().client();
            if (!path.startsWith("/")) {
                path = String.format("/%s", path);
            }

            String zkPath = String.format("%s%s", basePath(), path);
            if (client.checkExists().forPath(path) != null) {
                throw new IOException("Path already exists.");
            }
            DFSFileState fs = new DFSFileState();

            return fs;
        } catch (Exception ex) {
            throw new StateManagerError(String.format("Error creating new file entry. [path=%s]", path));
        }
    }

    public DFSFileState readFileHandle(@NonNull String path) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        try {
            CuratorFramework client = connection().client();

            String zkPath = String.format("%s%s", basePath(), path);
            if (client.checkExists().forPath(path) == null) {
                return null;
            }
            byte[] data = client.getData().forPath(zkPath);
            String jdata = new String(data, Charset.defaultCharset());

        } catch (Exception ex) {
            throw new StateManagerError(String.format("Error reading file entry. [path=%s]", path));
        }
        return null;
    }

    @Getter
    @Accessors(fluent = true)
    public static class FSStateManagerConfig extends ConfigReader {
        private static final class Constants {
            private static final String CONFIG_ZK_BASE = "basePath";
            private static final String CONFIG_ZK_CONNECTION = "connection";
        }

        private static final String __CONFIG_PATH = "state.manager";

        private String basePath;
        private String zkConnection;

        public FSStateManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                basePath = get().getString(Constants.CONFIG_ZK_BASE);
                if (Strings.isNullOrEmpty(basePath)) {
                    throw new ConfigurationException(String.format("State Manager Configuration Error: missing [%s]", Constants.CONFIG_ZK_BASE));
                }
                basePath = basePath.trim();
                if (basePath.endsWith("/")) {
                    basePath = basePath.substring(0, basePath.length() - 2);
                }
                zkConnection = get().getString(Constants.CONFIG_ZK_CONNECTION);
                if (Strings.isNullOrEmpty(zkConnection)) {
                    throw new ConfigurationException(String.format("State Manager Configuration Error: missing [%s]", Constants.CONFIG_ZK_CONNECTION));
                }
            } catch (Throwable t) {
                throw new ConfigurationException("Error processing State Manager configuration.", t);
            }
        }
    }
}
