package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.AbstractEnvState;
import ai.sapper.cdc.core.BaseEnv;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class DemoEnv extends BaseEnv<DemoEnv.EDemoState> {
    public DemoEnv() {
        super("demo");
    }

    public enum EDemoState {
        Error
    }

    public static class DemoState extends AbstractEnvState<EDemoState> {

        public DemoState() {
            super(EDemoState.Error);
        }

        @Override
        public boolean isAvailable() {
            return false;
        }
    }

    public static final String __CONFIG_PATH = "demo";
    private static final String CONFIG_CONNECTIONS = "connections.path";
    private static final String TEST_PASSWD = "test1234";

    private HierarchicalConfiguration<ImmutableNode> configNode;
    private final String module = "TEST";

    public BaseEnv<EDemoState> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        withStoreKey(TEST_PASSWD);
        super.init(xmlConfig, new DemoState());

        configNode = rootConfig().configurationAt(__CONFIG_PATH);

        return this;
    }
}
