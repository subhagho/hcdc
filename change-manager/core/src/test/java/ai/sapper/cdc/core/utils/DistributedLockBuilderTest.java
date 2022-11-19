package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class DistributedLockBuilderTest {
    private static final String __CONFIG_FILE = "src/test/resources/test-env.xml";
    private static XMLConfiguration xmlConfiguration = null;
    private static DemoEnv env = new DemoEnv();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }

    @Test
    void createLock() {
        try {
            try (DistributedLock lock = env.createLock("/test/env/demo/locks", env.module(), "LOCK_REPLICATION")) {
                assertNotNull(lock);
            }
            env.saveLocks();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}