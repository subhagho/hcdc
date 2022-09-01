package ai.sapper.hcdc.agents.model;

import ai.sapper.cdc.common.model.AvroChangeType;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class DFSReplicationDelta {
    private AvroChangeType.EChangeType op;
    private long inodeId;
    private long transactionId;
    private Map<String, String> fsPath;
    private long recordCount;
}
