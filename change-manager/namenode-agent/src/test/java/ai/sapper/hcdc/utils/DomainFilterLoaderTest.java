package ai.sapper.hcdc.utils;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.filters.DomainManager;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ProcessorStateManager;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DomainFilterLoaderTest {
    private static final String __CONFIG_PATH = "src/test/resources/configs/hcdc-agent.xml";
    private static final String TEST_DOMAIN_FILE = "src/test/resources/test-domain-loader.csv";

    private static XMLConfiguration xmlConfiguration = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_PATH, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void read() {
        try {
            String name = getClass().getSimpleName();
            NameNodeEnv.setup(name, getClass(), xmlConfiguration);
            DefaultLogger.LOGGER.info(
                    String.format("Name Node Agent environment initialized. [namespace=%s]",
                            NameNodeEnv.get(name).module()));
            assertNotNull(NameNodeEnv.get(name).stateManager());
            assertTrue(NameNodeEnv.get(name).stateManager() instanceof ProcessorStateManager);
            DomainManager domainManager = ((ProcessorStateManager) NameNodeEnv.get(name).stateManager()).domainManager();
            assertNotNull(domainManager);

            new DomainFilterLoader().read(TEST_DOMAIN_FILE, domainManager);
            NameNodeEnv.dispose(name);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}