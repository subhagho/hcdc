package ai.sapper.hcdc.core.schema;

import ai.sapper.hcdc.common.ConfigReader;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class SchemaManager {

    public static class SchemaManagerConfig extends ConfigReader {
        public static class Constants {
            public static final String __CONFIG_PATH = "managers.schema";
        }

        public SchemaManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                   @NonNull String path) {
            super(config, path);
        }
    }
}
