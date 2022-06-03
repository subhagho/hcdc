package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import ai.sapper.hcdc.agents.namenode.DFSAgentError;
import ai.sapper.hcdc.agents.namenode.model.DFSEditLogBatch;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.hadoop.hdfs.server.namenode.CustomEditsVisitor;

@Getter
@Accessors(fluent = true)
public class EditsLogViewer {
    private DFSEditLogBatch batch;
    private CustomEditsVisitor visitor;

    public void run(@NonNull String filename) throws DFSAgentError {
        try {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));

            visitor = new CustomEditsVisitor(filename);
            OfflineEditsLoader loader = OfflineEditsLoader.OfflineEditsLoaderFactory.
                    createLoader(visitor, filename, false, new OfflineEditsViewer.Flags());
            loader.loadEdits();

            batch = visitor.getBatch();
        } catch (Throwable t) {
            DefaultLogger.__LOG.debug(DefaultLogger.stacktrace(t));
            throw new DFSAgentError(t);
        }
    }
}
