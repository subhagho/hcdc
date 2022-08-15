package ai.sapper.hcdc.agents.model;

import ai.sapper.cdc.core.model.ModuleInstance;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class NameNodeTxState {
    private String namespace;
    private long updatedTime;
    private long processedTxId = 0;
    private ModuleInstance moduleInstance;
}
