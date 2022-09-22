package ai.sapper.cdc.core.connections;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.settngs.Setting;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class ConnectionConfig extends ConfigReader {
    public static final String CONFIG_NAME = "name";

    @Setting(name = CONFIG_NAME)
    private String name;
    @Setting(name = ConfigReader.__NODE_PARAMETERS, required = false)
    private Map<String, String> parameters;

    public ConnectionConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                            @NonNull String path) {
        super(config, path);
    }
}
