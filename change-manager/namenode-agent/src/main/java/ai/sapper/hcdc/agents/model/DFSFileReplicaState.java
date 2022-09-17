package ai.sapper.hcdc.agents.model;

import ai.sapper.cdc.common.schema.SchemaEntity;
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
    private DFSFileInfo fileInfo;
    private SchemaEntity entity;
    private String zkPath;
    private boolean enabled = false;
    private long snapshotTxId = -1;
    private long lastReplicatedTx = -1;
    private long snapshotTime;
    private boolean snapshotReady = false;
    private long lastReplicationTime;
    private long updateTime = 0;
    private long recordCount = 0;

    private EFileState state = EFileState.Unknown;
    private Map<String, String> storagePath;
    private Map<Long, DFSReplicationDelta> replicationDeltas;

    private List<DFSBlockReplicaState> blocks = new ArrayList<>();

    public void addDelta(@NonNull DFSReplicationDelta delta) {
        if (replicationDeltas == null) {
            replicationDeltas = new HashMap<>();
        }
        replicationDeltas.put(delta.getTransactionId(), delta);
    }

    public DFSReplicationDelta getDelta(long txId) {
        if (replicationDeltas != null) {
            return replicationDeltas.get(txId);
        }
        return null;
    }

    public DFSReplicationDelta removeDelta(long txId) {
        if (replicationDeltas != null && replicationDeltas.containsKey(txId)) {
            return replicationDeltas.remove(txId);
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
                copyBlock(bs);
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
