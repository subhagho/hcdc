package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NameNodeEnvTest {
    private static final String __CONFIG_FILE = "src/test/resources/namenode-agent-test.xml";
    private static final String __CONFIG_PATH = "agent";

    private static XMLConfiguration xmlConfiguration = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = TestUtils.readFile(__CONFIG_FILE);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void init() {
        try {
            NameNodeEnv.setup(xmlConfiguration, __CONFIG_PATH);
            DefaultLogger.__LOG.info(String.format("Name Node Agent environment initialized. [namespace=%s]", NameNodeEnv.get().namespace()));
            assertNotNull(NameNodeEnv.get().hdfsConnection());

            NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
            assertEquals(NameNodeEnv.ENameNEnvState.Disposed, state);
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}