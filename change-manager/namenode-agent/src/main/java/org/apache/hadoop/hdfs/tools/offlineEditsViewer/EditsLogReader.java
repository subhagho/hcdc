package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.hcdc.agents.namenode.DFSAgentError;
import ai.sapper.hcdc.agents.namenode.DFSEditsFileFinder;
import ai.sapper.hcdc.agents.namenode.model.DFSEditLogBatch;
import ai.sapper.hcdc.agents.namenode.model.DFSTransactionType;
import ai.sapper.hcdc.common.model.DFSTransaction;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.hadoop.hdfs.server.namenode.CustomEditsVisitor;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class EditsLogReader {
    private DFSEditLogBatch batch;
    private CustomEditsVisitor visitor;

    public void run(@NonNull DFSEditsFileFinder.EditsLogFile file, long startTxId, long endTxId) throws DFSAgentError {
        try {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(file.path()));

            visitor = new CustomEditsVisitor(file.path()).withStartTxId(startTxId).withEndTxId(endTxId);
            OfflineEditsLoader loader = OfflineEditsLoader.OfflineEditsLoaderFactory.
                    createLoader(visitor, file.path(), false, new OfflineEditsViewer.Flags());
            loader.loadEdits();

            DFSEditLogBatch b = visitor.getBatch();
            batch = new DFSEditLogBatch(b);
            long stx = Math.max(startTxId, file.startTxId());
            long etx = Math.min(endTxId, file.endTxId());
            if (b.transactions() == null || b.transactions().isEmpty()) {
                for (long ii = stx; ii <= etx; ii++) {
                    DFSTransactionType<?> tx = buildIgnoreTx(ii);
                    batch.transactions().add(tx);
                }
            } else {
                long lastTx = stx;
                for (DFSTransactionType<?> tx : b.transactions()) {
                    if (tx.id() > lastTx) {
                        for (long ii = lastTx + 1; ii < tx.id(); ii++) {
                            DFSTransactionType<?> ntx = buildIgnoreTx(ii);
                            batch.transactions().add(ntx);
                        }
                        batch.transactions().add(tx);
                        lastTx = tx.id();
                    }
                }
                if (lastTx < etx) {
                    for (long ii = lastTx + 1; ii < etx; ii++) {
                        DFSTransactionType<?> ntx = buildIgnoreTx(ii);
                        batch.transactions().add(ntx);
                    }
                }
            }
        } catch (Throwable t) {
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            throw new DFSAgentError(t);
        }
    }

    private DFSTransactionType<?> buildIgnoreTx(long txId) {
        DFSTransactionType.DFSIgnoreTxType ift = new DFSTransactionType.DFSIgnoreTxType();
        ift.id(txId).op(DFSTransaction.Operation.IGNORE);
        ift.opCode(FSEditLogOpCodes.OP_INVALID.name());

        return ift;
    }

    public void run(@NonNull DFSEditsFileFinder.EditsLogFile file) throws DFSAgentError {
        run(file, -1, -1);
    }

    public static List<DFSEditLogBatch> readEditsInRange(@NonNull String dir, long startTxId, long endTxId) throws DFSAgentError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dir));

        try {
            List<DFSEditLogBatch> batches = new ArrayList<>();
            List<DFSEditsFileFinder.EditsLogFile> files = DFSEditsFileFinder.findEditsFiles(dir, startTxId, endTxId);
            if (files != null && !files.isEmpty()) {
                for (DFSEditsFileFinder.EditsLogFile file : files) {
                    DefaultLogger.LOG.info(String.format("Reading transactions from edits file. [%s][startTx=%d, endTx=%d]", file.path(), startTxId, endTxId));
                    EditsLogReader viewer = new EditsLogReader();
                    viewer.run(file, startTxId, endTxId);

                    if (viewer.batch != null) {
                        batches.add(viewer.batch);
                    }
                }
            }
            if (!batches.isEmpty()) return batches;
            return null;
        } catch (Exception ex) {
            throw new DFSAgentError(ex);
        }
    }
}
