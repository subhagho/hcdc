package ai.sapper.hcdc.core.connections;

import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionManager {
    private static final String __PATH = "connections.connection";

    private String configPath;
    private XMLConfiguration config;
    private Map<String, String> connections = new HashMap<>();

    public ConnectionManager load(@NonNull XMLConfiguration config, String pathPrefix) throws ConnectionError {
        if (Strings.isNullOrEmpty(pathPrefix)) {
            configPath = __PATH;
        } else {
            configPath = String.format("%s.%s", pathPrefix, __PATH);
        }
        this.config = config;

        return this;
    }

    private List<HierarchicalConfiguration<ImmutableNode>> nodes() {

        return null;
    }
}
