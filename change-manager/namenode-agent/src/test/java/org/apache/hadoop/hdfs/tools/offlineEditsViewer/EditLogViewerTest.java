package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.hcdc.agents.common.DFSEditsFileFinder;
import ai.sapper.hcdc.agents.model.DFSEditLogBatch;
import ai.sapper.hcdc.agents.model.DFSTransactionType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EditLogViewerTest {
    private static final String EDITS_FILE_01 = "src/test/resources/edits/logs/edits_0000000000000022789-0000000000000022790";
    private static final String EDITS_FILE_CURRENT = "src/test/resources/edits/edits_inprogress_0000000000000001594";
    private static final String SOURCE_DIR = "src/test/resources/edits";

    @Test
    void run() {
        try {
            DFSEditsFileFinder.EditsLogFile file = DFSEditsFileFinder.parseFileName(EDITS_FILE_01);
            assertNotNull(file);
            EditsLogFileReader viewer = new EditsLogFileReader();
            viewer.run(file);

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
                DefaultLogger.LOG.info(tnx.toString());
                lastTxId = tnx.id();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t);
        }
    }

    @Test
    void runFor() {
        try {
            List<DFSEditLogBatch> batches = EditsLogFileReader.readEditsInRange(SOURCE_DIR, 7, 24);
            assertNotNull(batches);
            for (DFSEditLogBatch batch : batches) {
                List<DFSTransactionType<?>> transactions = batch.transactions();
                assertNotNull(transactions);
                if (transactions.size() > 0) {
                    DefaultLogger.LOG.info(String.format("Transactions found in edits file. [file=%s]", batch.filename()));
                    for (DFSTransactionType<?> tnx : transactions) {
                        DefaultLogger.LOG.info(tnx.toString());
                    }
                } else {
                    DefaultLogger.LOG.warn(String.format("No Transactions found in edits file. [file=%s]", batch.filename()));
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t);
        }
    }
}