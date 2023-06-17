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

package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DFSEditsFileFinder;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.model.dfs.DFSEditLogBatch;
import ai.sapper.cdc.core.model.dfs.DFSTransactionType;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EditLogViewerTest {
    private static final String __CONFIG_FILE = "src/test/resources/configs/namenode-agent.xml";

    private static final String EDITS_FILE_01 = "src/test/resources/edits/logs/edits_0000000000000022789-0000000000000022790";
    private static final String EDITS_FILE_CURRENT = "src/test/resources/edits/edits_inprogress_0000000000000001594";
    private static final String SOURCE_DIR = "src/test/resources/edits";

    private static NameNodeEnv env;

    @BeforeAll
    public static void setup() throws Exception {
        XMLConfiguration xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);

        String name = EditLogViewerTest.class.getSimpleName();

        env = NameNodeEnv.setup(name, EditLogViewerTest.class, xmlConfiguration);
    }

    @Test
    void run() {
        try {
            DFSEditsFileFinder.EditsLogFile file = DFSEditsFileFinder.parseFileName(EDITS_FILE_01);
            assertNotNull(file);
            EditsLogFileReader viewer = new EditsLogFileReader();
            viewer.run(file, env);

            DFSEditLogBatch batch = viewer.batch();
            assertNotNull(batch);
            List<DFSTransactionType<?>> transactions = batch.transactions();
            assertNotNull(transactions);
            assertTrue(transactions.size() > 0);
            long lastTxId = -1;
            for (DFSTransactionType<?> tnx : transactions) {
                if (lastTxId > 0) {
                    assertEquals((lastTxId + 1), tnx.id());
                }
                DefaultLogger.info(tnx.toString());
                lastTxId = tnx.id();
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }

    @Test
    void runFor() {
        try {
            List<DFSEditLogBatch> batches = EditsLogFileReader.readEditsInRange(SOURCE_DIR, 7, 24, env);
            assertNotNull(batches);
            for (DFSEditLogBatch batch : batches) {
                List<DFSTransactionType<?>> transactions = batch.transactions();
                assertNotNull(transactions);
                if (transactions.size() > 0) {
                    DefaultLogger.info(
                            String.format("Transactions found in edits file. [file=%s]", batch.filename()));
                    for (DFSTransactionType<?> tnx : transactions) {
                        DefaultLogger.info(tnx.toString());
                    }
                } else {
                    DefaultLogger.warn(
                            String.format("No Transactions found in edits file. [file=%s]", batch.filename()));
                }
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}