package ai.sapper.hcdc.agents.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class SnapshotState {
    private String module;
    private long snapshotTxId = -1;
    private long snapshotSeq = -1;
    private long updatedTimestamp = -1;
}
