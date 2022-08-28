package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.common.DFSAgentError;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.model.DFSEditLogBatch;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsVisitor;

import java.io.IOException;

@Getter
public class CustomEditsVisitor implements OfflineEditsVisitor {
    private int version;
    private long startTxId = -1;
    private long endTxId = -1;
    private final DFSEditLogBatch batch;
    private DFSEditLogParser parser;
    private final NameNodeEnv env;

    public CustomEditsVisitor(@NonNull String filename, @NonNull NameNodeEnv env) {
        this.env = env;
        batch = new DFSEditLogBatch(filename);
    }

    public CustomEditsVisitor withStartTxId(long startTxId) {
        this.startTxId = startTxId;
        return this;
    }

    public CustomEditsVisitor withEndTxId(long endTxId) {
        this.endTxId = endTxId;
        return this;
    }

    /**
     * Begin visiting the edits log structure.  Opportunity to perform
     * any initialization necessary for the implementing visitor.
     *
     * @param version Edit log version
     */
    @Override
    public void start(int version) throws IOException {
        this.version = version;

        batch.version(version);
        batch.setup();

        parser = new DFSEditLogParser()
                .withStartTxId(startTxId)
                .withEndTxId(endTxId)
                .withEnv(env);
    }

    /**
     * Finish visiting the edits log structure.  Opportunity to perform any
     * clean up necessary for the implementing visitor.
     *
     * @param error If the visitor was closed because of an
     *              unrecoverable error in the input stream, this
     *              is the exception.
     */
    @Override
    public void close(Throwable error) throws IOException {

    }

    /**
     * Begin visiting an element that encloses another element, such as
     * the beginning of the list of blocks that comprise a file.
     *
     * @param op
     */
    @Override
    public void visitOp(FSEditLogOp op) throws IOException {
        try {
            parser.parse(op, batch);
        } catch (DFSAgentError e) {
            DefaultLogger.LOGGER.error(String.format("Error parsing OP Code [%s]", op.opCode.name()), e);
            throw new IOException(e);
        }
    }
}
