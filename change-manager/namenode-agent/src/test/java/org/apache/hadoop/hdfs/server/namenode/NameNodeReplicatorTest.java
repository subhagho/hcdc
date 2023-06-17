/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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