package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.hcdc.agents.namenode.model.DFSEditLogBatch;
import ai.sapper.hcdc.agents.namenode.model.DFSTransactionType;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EditLogViewerTest {
    private static final String EDITS_FILE_01 = "src/test/resources/edits_0000000000000000315-0000000000000000328";
    private static final String EDITS_FILE_CURRENT = "src/test/resources/edits_inprogress_0000000000000001594";
    private static final String SOURCE_DIR = "src/test/resources/edits";

    @Test
    void run() {
        try {
            EditsLogViewer viewer = new EditsLogViewer();
            viewer.run(EDITS_FILE_01);

            DFSEditLogBatch batch = viewer.batch();
            assertNotNull(batch);
            List<DFSTransactionType<?>> transactions = batch.transactions();
            assertNotNull(transactions);
            assertTrue(transactions.size() > 0);
            for (DFSTransactionType<?> tnx : transactions) {
                DefaultLogger.__LOG.info(tnx.toString());
            }
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t);
        }
    }

    @Test
    void runFor() {
        try {
            List<DFSEditLogBatch> batches = EditsLogViewer.readEditsInRange(SOURCE_DIR, 7, 24);
            assertNotNull(batches);
            for (DFSEditLogBatch batch : batches) {
                List<DFSTransactionType<?>> transactions = batch.transactions();
                assertNotNull(transactions);
                if (transactions.size() > 0) {
                    DefaultLogger.__LOG.info(String.format("Transactions found in edits file. [file=%s]", batch.filename()));
                    for (DFSTransactionType<?> tnx : transactions) {
                        DefaultLogger.__LOG.info(tnx.toString());
                    }
                } else {
                    DefaultLogger.__LOG.warn(String.format("No Transactions found in edits file. [file=%s]", batch.filename()));
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            fail(t);
        }
    }
}