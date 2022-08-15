package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.agents.namenode.main.NameNodeReplicator;
import ai.sapper.cdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NameNodeReplicatorTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";

    @Test
    void run() {
        try {
            System.setProperty("hadoop.home.dir", "C:/tools/hadoop");

            String[] args = {"--image",
                    "src/test/resources/edits/logs/current/fsimage_0000000000000028553",
                    "--config",
                    __CONFIG_FILE,
                    "--tmp",
                    "/Work/temp/output/test"};
            NameNodeReplicator.main(args);

        } catch (Throwable t) {
            DefaultLogger.LOG.error(DefaultLogger.stacktrace(t));
            fail(t);
        }
    }
}