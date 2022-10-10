package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.main.NameNodeReplicator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

class NameNodeReplicatorTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-replicator.xml";

    @Test
    void run() {
        try {
            System.setProperty("hadoop.home.dir", "/opt/hadoop/hadoop");

            String[] args = {"--imageDir",
                    "/data01/hadoop/nn-primary/nn-primary-data/namenode/current/",
                    "--config",
                    __CONFIG_FILE,
                    "--tmp",
                    "/tmp/output/test"};
            NameNodeReplicator.main(args);

        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}