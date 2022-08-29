package ai.sapper.hcdc.agents.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
public class DFSBlockReplicaState {
    private long blockId;
    private long prevBlockId = -1;
    private long updateTime = 0;
    private EFileState state = EFileState.Unknown;
    private Map<String, String> storagePath;
    private long startOffset;
    private long dataSize;

    public boolean canUpdate() {
        return (state == EFileState.New || state == EFileState.Updating);
    }
}
