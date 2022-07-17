package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.agents.namenode.NameNodeEnv;
import ai.sapper.hcdc.agents.namenode.NameNodeReplicator;
import ai.sapper.hcdc.agents.namenode.ZkStateManager;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.model.DFSFileState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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

            ZkStateManager stateManager = NameNodeEnv.stateManager();
            assertNotNull(stateManager);
            DFSFileState fileState = stateManager.get("/test/hcdc/loader/parquet/tags/2022/06/13/11/tags_54.parquet");
            assertNotNull(fileState);
            
            NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
            assertEquals(NameNodeEnv.ENameNEnvState.Disposed, state);
        } catch (Throwable t) {
            DefaultLogger.LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}