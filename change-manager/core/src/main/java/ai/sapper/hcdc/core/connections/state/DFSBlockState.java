package ai.sapper.hcdc.core.connections.state;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DFSBlockState {
    private long blockId;
    private long createdTime;
    private long updatedTime;
    private long dataSize = 0;
    private long lastTnxId;

    @Getter
    @Setter
    public static class BlockTnxDelta {
        private long startOffset;
        private long endOffset;
        private long tnxId;
    }
}
