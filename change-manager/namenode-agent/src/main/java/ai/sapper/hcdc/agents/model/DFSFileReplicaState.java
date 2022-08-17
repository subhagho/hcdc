package ai.sapper.hcdc.agents.model;

import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.cdc.core.model.DFSBlockState;
import ai.sapper.cdc.core.model.DFSFileState;
import ai.sapper.cdc.core.model.EFileState;
import ai.sapper.cdc.core.model.EFileType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
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
    private Map<Long, Map<String, String>> tnxDeltaPaths;
    private EFileType fileType = EFileType.UNKNOWN;
    private String schemaLocation;

    private List<DFSBlockReplicaState> blocks = new ArrayList<>();

    public void addDelta(long tnxId, @NonNull Map<String, String> fsPath) {
        if (tnxDeltaPaths == null) {
            tnxDeltaPaths = new HashMap<>();
        }
        tnxDeltaPaths.put(tnxId, fsPath);
    }

    public Map<String, String> getDelta(long txId) {
        if (tnxDeltaPaths != null) {
            return tnxDeltaPaths.get(txId);
        }
        return null;
    }

    public Map<String, String> removeDelta(long txId) {
        if (tnxDeltaPaths != null && tnxDeltaPaths.containsKey(txId)) {
            return tnxDeltaPaths.remove(txId);
        }
        return null;
    }

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

    public DFSFileReplicaState copyBlocks(@NonNull DFSFileState fileState) throws Exception {
        if (fileState.hasBlocks()) {
            for (DFSBlockState bs : fileState.getBlocks()) {
                DFSBlockReplicaState b = new DFSBlockReplicaState();
                b.setState(EFileState.New);
                b.setBlockId(bs.getBlockId());
                b.setPrevBlockId(bs.getPrevBlockId());
                b.setStartOffset(0);
                b.setDataSize(bs.getDataSize());
                b.setUpdateTime(System.currentTimeMillis());
                add(b);
            }
        }
        return this;
    }

    public DFSFileReplicaState copyBlock(@NonNull DFSBlockState bs) throws Exception {
        DFSBlockReplicaState b = new DFSBlockReplicaState();
        b.setState(EFileState.New);
        b.setBlockId(bs.getBlockId());
        b.setPrevBlockId(bs.getPrevBlockId());
        b.setStartOffset(0);
        b.setDataSize(bs.getDataSize());
        b.setUpdateTime(System.currentTimeMillis());
        add(b);

        return this;
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
