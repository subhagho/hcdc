package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.namenode.main.NameNodeReplicator;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NameNodeReplicatorTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";

    @Test
    void run() {
        try {
            System.setProperty("hadoop.home.dir", "C:/tools/hadoop");

            String[] args = {"--image",
                    "src/test/resources/edits/logs/current/fsimage_0000000000000026172",
                    "--config",
                    __CONFIG_FILE,
                    "--tmp",
                    "/Work/temp/output/test"};
            NameNodeReplicator.main(args);

            ZkStateManager stateManager = NameNodeEnv.stateManager();
            assertNotNull(stateManager);

            NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
            assertEquals(NameNodeEnv.ENameNEnvState.Disposed, state);
        } catch (Throwable t) {
            DefaultLogger.LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}