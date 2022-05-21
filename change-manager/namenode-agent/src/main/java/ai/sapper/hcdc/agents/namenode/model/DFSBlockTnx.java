package ai.sapper.hcdc.agents.namenode.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

@Getter
@Setter
public class DFSBlockTnx {
    private long startTnxId = Long.MAX_VALUE;
    private long endTnxId = -1;
    private FSEditLogOpCodes opCode;
    private long blockId;
    private long blockSize;
    private long numBytes;
    private boolean overwrite;
    private long genstamp;
}
