package ai.sapper.hcdc.agents.namenode.model;

import ai.sapper.hcdc.common.model.SchemaEntity;
import ai.sapper.hcdc.core.model.EFileState;
import ai.sapper.hcdc.core.model.EFileType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class DFSFileReplicaState {
    private long inode;
    private String hdfsPath;
    private SchemaEntity entity;
    private String zkPath;
    private boolean enabled = false;
    private long snapshotTxId = -1;
    private long lastReplicatedTx = -1;
    private long snapshotTime;
    private boolean snapshotReady = false;
    private long lastReplicationTime;
    private long updateTime = 0;
    private EFileState state = EFileState.Unknown;
    private Map<String, String> storagePath;
    private Map<String, String> lastDeltaPath;
    private EFileType fileType = EFileType.UNKNOWN;

    private List<DFSBlockReplicaState> blocks = new ArrayList<>();

    public void add(@NonNull DFSBlockReplicaState block) throws Exception {
        DFSBlockReplicaState b = get(block.getBlockId());
        if (b != null) {
            throw new Exception(String.format("Block already exists. [id=%d]", block.getBlockId()));
        }
        blocks.add(block);
    }

    public DFSBlockReplicaState get(long blockId) {
        for (DFSBlockReplicaState b : blocks) {
            if (b.getBlockId() == blockId) {
                return b;
            }
        }
        return null;
    }

    public boolean hasBlocks() {
        return !blocks.isEmpty();
    }

    public boolean canUpdate() {
        return (state == EFileState.New || state == EFileState.Updating);
    }

    public void clear() {
        // blocks.clear();
    }
}
