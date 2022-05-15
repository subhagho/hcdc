package ai.sapper.hcdc.core.connections;

import ai.sapper.hcdc.common.ConfigReader;
import lombok.NonNull;
import org.apache.commons.configuration2.XMLConfiguration;

public class HdfsConnection {
    public static class HdfsConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "connection.hdfs";

        public HdfsConfig(@NonNull XMLConfiguration config) {
            super(config, __CONFIG_PATH);
        }
    }
}
