package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.namenode.model.DFSEditLogBatch;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.text.ParseException;

@Getter
@Accessors(fluent = true)
public class DFSEditLogParser {
    public void parse(FSEditLogOp op, DFSEditLogBatch batch) throws ParseException {

    }
}
