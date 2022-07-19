package ai.sapper.hcdc.agents.namenode.main;

import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EditLogRunnerTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";
    @Test
    void runOnce() {
        try {
            EditLogRunner runner = new EditLogRunner();
            runner.runOnce(__CONFIG_FILE);
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}