package ai.sapper.hcdc.agents.namenode.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NameNodeAgentState {
    private String namespace;
    private long updatedTime;
    private long lastTxId;
}
