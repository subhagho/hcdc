package ai.sapper.cdc.core;

import ai.sapper.cdc.core.schema.SchemaManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.NotImplementedException;

@Getter
@Accessors(fluent = true)
public class DemoEnv extends BaseEnv<DemoEnv.DemoState> {
    public DemoEnv() {
        super("demo");
    }

    @Override
    public <S extends SchemaManager> S schemaManager(@NonNull Class<? extends SchemaManager> type) throws Exception {
        throw new NotImplementedException("Should not be called...");
    }

    public static class DemoState {

    }

    public static final String __CONFIG_PATH = "demo";
    private static final String CONFIG_CONNECTIONS = "connections.path";
    private static final String TEST_PASSWD = "test1234";

    private HierarchicalConfiguration<ImmutableNode> configNode;
    private final String module = "TEST";

    public BaseEnv<DemoState> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        withStoreKey(TEST_PASSWD);
        super.init(xmlConfig, new DemoState());

        configNode = rootConfig().configurationAt(__CONFIG_PATH);

        return this;
    }
}
