package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NameNodeEnvTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";

    private static XMLConfiguration xmlConfiguration = null;
    private static int lockCounter = 0;

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

    @Test
    void testLocking() {
        try {
            NameNodeEnv.setup(xmlConfiguration);
            DefaultLogger.LOG.info(String.format("Name Node Agent environment initialized. [source=%s]", NameNodeEnv.get().source()));
            assertNotNull(NameNodeEnv.get().hdfsConnection());
            assertNotNull(NameNodeEnv.stateManager().connection());
            Thread[] threads = new Thread[5];
            for (int ii = 0; ii < 5; ii++) {
                threads[ii] = new Thread(new Locker());
                threads[ii].start();
            }
            for (int ii = 0; ii < 5; ii++) {
                threads[ii].join();
            }
        } catch (Throwable t) {
            DefaultLogger.LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }

    private static class Locker implements Runnable {

        @Override
        public void run() {
            try {
                Thread.sleep(500);
                try (DistributedLock gl = NameNodeEnv.globalLock()) {
                    try (DistributedLock lock = NameNodeEnv.get().createLock("LOCK_REPLICATION")) {
                        for (int ii = 0; ii < 5; ii++) {
                            gl.lock();
                            try {
                                lock.lock();
                                try {
                                    lock.lock(); // Checking re-entrance
                                    DefaultLogger.LOG.info(String.format("LOCK COUNTER=%d", lockCounter++));
                                } finally {
                                    lock.unlock();
                                }
                            } finally {
                                gl.unlock();
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                DefaultLogger.LOG.error(DefaultLogger.stacktrace(t));
                fail(t);
            }
        }
    }
}