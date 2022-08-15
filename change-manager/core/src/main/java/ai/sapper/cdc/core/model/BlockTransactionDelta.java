package ai.sapper.cdc.core.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BlockTransactionDelta {
    private long startOffset = 0;
    private long endOffset = 0;
    private long tnxId;
    private long timestamp;
    private boolean deleted = false;
}
