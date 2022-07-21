package ai.sapper.hcdc.utils;

import ai.sapper.hcdc.agents.namenode.NameNodeEnv;
import ai.sapper.hcdc.agents.namenode.ProcessorStateManager;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.filters.DomainManager;
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
        xmlConfiguration = ConfigReader.read(__CONFIG_PATH);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void read() {
        try {
            NameNodeEnv.setup(xmlConfiguration);
            DefaultLogger.LOG.info(String.format("Name Node Agent environment initialized. [namespace=%s]", NameNodeEnv.get().hadoopNamespace()));
            assertNotNull(NameNodeEnv.stateManager());
            assertTrue(NameNodeEnv.stateManager() instanceof ProcessorStateManager);
            DomainManager domainManager = ((ProcessorStateManager) NameNodeEnv.stateManager()).domainManager();
            assertNotNull(domainManager);

            new DomainFilterLoader().read(TEST_DOMAIN_FILE, domainManager);
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}