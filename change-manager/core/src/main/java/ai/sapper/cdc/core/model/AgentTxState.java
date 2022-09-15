package ai.sapper.cdc.core.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class AgentTxState {
    private String namespace;
    private long updatedTime;
    private String currentMessageId;
    private long processedTxId = 0;
    private ModuleInstance moduleInstance;
}
