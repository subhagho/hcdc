package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
import lombok.NonNull;
import org.apache.commons.configuration2.XMLConfiguration;

public class ZkConnection {
    public static final class ZkConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "connection.zookeeper";

        public ZkConfig(@NonNull XMLConfiguration config) {
            super(config, __CONFIG_PATH);
        }
    }
}
