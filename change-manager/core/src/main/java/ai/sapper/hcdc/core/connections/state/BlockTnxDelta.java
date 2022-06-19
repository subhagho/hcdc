package ai.sapper.hcdc.core.connections.state;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BlockTnxDelta {
    private long startOffset;
    private long endOffset;
    private long tnxId;
    private long timestamp;
}
