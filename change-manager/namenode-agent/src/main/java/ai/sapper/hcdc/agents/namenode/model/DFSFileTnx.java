package ai.sapper.hcdc.agents.namenode.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class DFSFileTnx {
    private long startTnxId = Long.MAX_VALUE;
    private long endTnxId = -1;
    private FSEditLogOpCodes opCode;
    private String path;
    private long accessTimestamp;
    private long modifiedTimestamp;

    private final Map<Long, DFSBlockTnx> blockTransactions = new HashMap<>();
}
