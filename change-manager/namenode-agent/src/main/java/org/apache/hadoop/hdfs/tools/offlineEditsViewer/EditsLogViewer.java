package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.hcdc.agents.namenode.DFSAgentError;
import ai.sapper.hcdc.agents.namenode.DFSEditsFileFinder;
import ai.sapper.hcdc.agents.namenode.model.DFSEditLogBatch;
import ai.sapper.hcdc.common.utils.DefaultLogger;
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
public class EditsLogViewer {
    private DFSEditLogBatch batch;
    private CustomEditsVisitor visitor;

    public void run(@NonNull String filename, long startTxId, long endTxId) throws DFSAgentError {
        try {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));

            visitor = new CustomEditsVisitor(filename).withStartTxId(startTxId).withEndTxId(endTxId);
            OfflineEditsLoader loader = OfflineEditsLoader.OfflineEditsLoaderFactory.
                    createLoader(visitor, filename, false, new OfflineEditsViewer.Flags());
            loader.loadEdits();

            batch = visitor.getBatch();
        } catch (Throwable t) {
            DefaultLogger.__LOG.debug(DefaultLogger.stacktrace(t));
            throw new DFSAgentError(t);
        }
    }

    public void run(@NonNull String filename) throws DFSAgentError {
        run(filename, -1, -1);
    }

    public static List<DFSEditLogBatch> runFor(@NonNull String dir, long startTxId, long endTxId) throws DFSAgentError {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dir));

        try {
            List<DFSEditLogBatch> batches = new ArrayList<>();
            List<String> files = DFSEditsFileFinder.findEditsFiles(dir, startTxId, endTxId);
            if (files != null && !files.isEmpty()) {
                for (String file : files) {
                    DefaultLogger.__LOG.info(String.format("Reading transactions from edits file. [%s][startTx=%d, endTx=%d]", file, startTxId, endTxId));
                    EditsLogViewer viewer = new EditsLogViewer();
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
