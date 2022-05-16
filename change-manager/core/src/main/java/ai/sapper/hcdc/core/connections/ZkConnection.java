package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class ZkConnection implements Connection {
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

    public static final class ZkConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "connection.zookeeper";

        public ZkConfig(@NonNull XMLConfiguration config, String pathPrefix) {
            super(config, __CONFIG_PATH, pathPrefix);
        }
    }
}
