package ai.sapper.hcdc.agents.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ModuleTxState {
    private String module;
    private long receivedTxId = -1;
    private long committedTxId = -1;
    private long snapshotTxId = -1;
    private long updateTimestamp;
    private String agentInstance;
}
