package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class ZkStateManager {
    private ZookeeperConnection connection;
    private ZkStateManagerConfig config;

    public ZkStateManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull ConnectionManager manger) throws NameNodeError {
        try {
            config = new ZkStateManagerConfig(xmlConfig);
            config.read();

            connection = manger.getConnection(config.zkConnection, ZookeeperConnection.class);

            return this;
        } catch (Exception ex) {
            throw new NameNodeError(ex);
        }
    }

    public String basePath() {
        return config().basePath();
    }

    @Getter
    @Accessors(fluent = true)
    public static class ZkStateManagerConfig extends ConfigReader {
        private static final class Constants {
            private static final String CONFIG_ZK_BASE = "basePath";
            private static final String CONFIG_ZK_CONNECTION = "connection";
        }

        private static final String __CONFIG_PATH = "state.manager";

        private String basePath;
        private String zkConnection;

        public ZkStateManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
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
