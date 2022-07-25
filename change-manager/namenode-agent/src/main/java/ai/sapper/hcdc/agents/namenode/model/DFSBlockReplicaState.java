package ai.sapper.hcdc.agents.namenode.model;

import ai.sapper.hcdc.core.model.EFileState;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DFSBlockReplicaState {
    private long blockId;
    private long prevBlockId = -1;
    private long updateTime = 0;
    private EFileState state = EFileState.Unknown;
    private String storagePath;
    private long startOffset;
    private long dataSize;

    public boolean canUpdate() {
        return (state == EFileState.New || state == EFileState.Updating);
    }
}
