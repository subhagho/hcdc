package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.model.Heartbeat;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NameNodeEnvTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";

    private static XMLConfiguration xmlConfiguration = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void init() {
        try {
            NameNodeEnv.setup(xmlConfiguration);
            DefaultLogger.LOG.info(String.format("Name Node Agent environment initialized. [source=%s]", NameNodeEnv.get().source()));
            assertNotNull(NameNodeEnv.get().hdfsConnection());
            assertNotNull(NameNodeEnv.stateManager().connection());

            NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
            assertEquals(NameNodeEnv.ENameNEnvState.Disposed, state);
        } catch (Throwable t) {
            DefaultLogger.LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}