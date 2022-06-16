package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.agents.namenode.NameNodeEnv;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class NameNodeReplicatorTest {
    private static final String __CONFIG_FILE = "src/test/resources/namenode-agent-test.xml";

    @Test
    void run() {
        try {
            System.setProperty("hadoop.home.dir", "C:/tools/hadoop");

            String[] args = {"--image",
                    "src/test/resources/fsimage_0000000000000007157",
                    "--config",
                    __CONFIG_FILE,
                    "--tmp",
                    "/Work/temp/output/test"};
            NameNodeReplicator.main(args);

            NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
            assertEquals(NameNodeEnv.ENameNEnvState.Disposed, state);
        } catch (Throwable t) {
            DefaultLogger.__LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}