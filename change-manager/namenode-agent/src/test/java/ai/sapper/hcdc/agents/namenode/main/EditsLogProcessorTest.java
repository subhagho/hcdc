package ai.sapper.hcdc.agents.namenode.main;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.main.EditsLogProcessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

class EditsLogProcessorTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";
    @Test
    void runOnce() {
        try {
            EditsLogProcessor runner = new EditsLogProcessor();
            runner.runOnce(__CONFIG_FILE);
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}