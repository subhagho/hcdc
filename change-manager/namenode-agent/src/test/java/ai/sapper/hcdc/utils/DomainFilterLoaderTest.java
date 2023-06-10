package ai.sapper.hcdc.utils;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
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
            NameNodeEnv env = NameNodeEnv.setup(name, getClass(), xmlConfiguration);
            DefaultLogger.info(
                    String.format("Name Node Agent environment initialized. [namespace=%s]",
                            env.module()));
            assertNotNull(env.schemaManager());
            HCdcSchemaManager schemaManager = env.schemaManager();

            new DomainFilterLoader().read(TEST_DOMAIN_FILE, schemaManager);
            NameNodeEnv.dispose(name);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}