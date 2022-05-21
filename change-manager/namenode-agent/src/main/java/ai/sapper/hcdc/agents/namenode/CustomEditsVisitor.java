package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.namenode.model.DFSEditLogBatch;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import lombok.Getter;
import lombok.NonNull;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsVisitor;

import java.io.IOException;
import java.text.ParseException;

@Getter
public class CustomEditsVisitor implements OfflineEditsVisitor {
    private int version;
    private final DFSEditLogBatch batch;
    private DFSEditLogParser parser;

    public CustomEditsVisitor(@NonNull String filename) {
        batch = new DFSEditLogBatch(filename);
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

        batch.setVersion(version);
        batch.setup();

        parser = new DFSEditLogParser();
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
        } catch (ParseException e) {
            DefaultLogger.__LOG.error(String.format("Error parsing OP Code [%s]", op.opCode.name()), e);
            throw new IOException(e);
        }
    }
}
