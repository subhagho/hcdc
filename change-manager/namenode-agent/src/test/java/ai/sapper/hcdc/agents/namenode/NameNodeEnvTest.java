package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.model.Heartbeat;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NameNodeEnvTest {
    private static final String __CONFIG_FILE = "src/test/resources/namenode-agent-test.xml";

    private static XMLConfiguration xmlConfiguration = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void init() {
        try {
            NameNodeEnv.setup(xmlConfiguration);
            DefaultLogger.LOG.info(String.format("Name Node Agent environment initialized. [namespace=%s]", NameNodeEnv.get().namespace()));
            assertNotNull(NameNodeEnv.get().hdfsConnection());
            assertNotNull(NameNodeEnv.stateManager().connection());

            Heartbeat hb = NameNodeEnv.stateManager().heartbeat(NameNodeEnv.get().config().nameNodeInstanceName());
            assertNotNull(hb);

            NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
            assertEquals(NameNodeEnv.ENameNEnvState.Disposed, state);
        } catch (Throwable t) {
            DefaultLogger.LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}