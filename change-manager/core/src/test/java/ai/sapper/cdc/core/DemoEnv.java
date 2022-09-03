package ai.sapper.cdc.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class DemoEnv extends BaseEnv {
    public static final String __CONFIG_PATH = "agent";
    private static final String CONFIG_CONNECTIONS = "connections.path";
    private static final String TEST_PASSWD = "test1234";

    private HierarchicalConfiguration<ImmutableNode> config;

    public DemoEnv init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        withStoreKey(TEST_PASSWD);
        config = xmlConfig.configurationAt(__CONFIG_PATH);
        String cp = config.getString(CONFIG_CONNECTIONS);
        Preconditions.checkState(!Strings.isNullOrEmpty(cp));
        super.init(xmlConfig, "TEST", cp);

        return this;
    }
}