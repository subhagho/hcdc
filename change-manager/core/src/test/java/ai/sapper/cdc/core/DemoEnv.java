package ai.sapper.cdc.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class DemoEnv extends BaseEnv<DemoEnv.DemoState> {
    public static class DemoState {

    }
    public static final String __CONFIG_PATH = "agent";
    private static final String CONFIG_CONNECTIONS = "connections.path";
    private static final String TEST_PASSWD = "test1234";

    private HierarchicalConfiguration<ImmutableNode> config;
    private final String module = "TEST";

    public BaseEnv<DemoState> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        super.init(xmlConfig, new DemoState());

        withStoreKey(TEST_PASSWD);
        config = rootConfig().configurationAt(__CONFIG_PATH);

        String cp = config.getString(CONFIG_CONNECTIONS);
        Preconditions.checkState(!Strings.isNullOrEmpty(cp));
        super.setup(module, cp);

        return this;
    }
}
