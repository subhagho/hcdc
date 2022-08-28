package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.common.DFSAgentError;
import ai.sapper.hcdc.agents.common.DFSEditsFileFinder;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.model.DFSEditLogBatch;
import ai.sapper.hcdc.agents.model.DFSTransactionType;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.hadoop.hdfs.server.namenode.CustomEditsVisitor;

import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class EditsLogFileReader {
    private DFSEditLogBatch batch;
    private CustomEditsVisitor visitor;

    public void run(@NonNull DFSEditsFileFinder.EditsLogFile file,
                    long startTxId,
                    long endTxId,
                    @NonNull NameNodeEnv env) throws DFSAgentError {
        try {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(file.path()));

            visitor = new CustomEditsVisitor(file.path(), env)
                    .withStartTxId(startTxId)
                    .withEndTxId(endTxId);
            OfflineEditsLoader loader = OfflineEditsLoader.OfflineEditsLoaderFactory.
                    createLoader(visitor, file.path(), false, new OfflineEditsViewer.Flags());
            loader.loadEdits();

            DFSEditLogBatch b = visitor.getBatch();
            batch = new DFSEditLogBatch(b);
            if (startTxId < 0) {
                startTxId = file.startTxId();
            }
            if (endTxId < 0) {
                endTxId = file.endTxId();
            }
            long stx = Math.max(startTxId, file.startTxId());
            long etx = Math.min(endTxId, file.endTxId());

            for (DFSTransactionType<?> tx : b.transactions()) {
                batch.transactions().add(tx);
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new DFSAgentError(t);
        }
    }

    public void run(@NonNull DFSEditsFileFinder.EditsLogFile file, @NonNull NameNodeEnv env) throws DFSAgentError {
        run(file, -1, -1, env);
    }

    public static List<DFSEditLogBatch> readEditsInRange(@NonNull String dir,
                                                         long startTxId,
                                                         long endTxId,
                                                         @NonNull NameNodeEnv env) throws DFSAgentError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dir));

        try {
            List<DFSEditLogBatch> batches = new ArrayList<>();
            List<DFSEditsFileFinder.EditsLogFile> files = DFSEditsFileFinder.findEditsFiles(dir, startTxId, endTxId);
            if (files != null && !files.isEmpty()) {
                for (DFSEditsFileFinder.EditsLogFile file : files) {
                    DefaultLogger.LOGGER.info(
                            String.format("Reading transactions from edits file. [%s][startTx=%d, endTx=%d]",
                                    file.path(), startTxId, endTxId));
                    EditsLogFileReader viewer = new EditsLogFileReader();
                    viewer.run(file, startTxId, endTxId, env);

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
