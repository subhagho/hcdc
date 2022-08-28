package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.common.DFSEditsFileFinder;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.model.DFSEditLogBatch;
import ai.sapper.hcdc.agents.model.DFSTransactionType;
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
                DefaultLogger.LOGGER.info(tnx.toString());
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
                    DefaultLogger.LOGGER.info(
                            String.format("Transactions found in edits file. [file=%s]", batch.filename()));
                    for (DFSTransactionType<?> tnx : transactions) {
                        DefaultLogger.LOGGER.info(tnx.toString());
                    }
                } else {
                    DefaultLogger.LOGGER.warn(
                            String.format("No Transactions found in edits file. [file=%s]", batch.filename()));
                }
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}