package ai.sapper.hcdc.agents.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SnapshotState {
    private String module;
    private long snapshotTxId = -1;
    private long snapshotSeq = -1;
    private long updatedTimestamp = -1;
}
