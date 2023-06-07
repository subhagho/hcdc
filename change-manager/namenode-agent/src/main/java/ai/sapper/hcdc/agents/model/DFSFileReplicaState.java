package ai.sapper.hcdc.agents.model;

import ai.sapper.cdc.core.model.EFileReplicationState;
import ai.sapper.cdc.core.model.EFileState;
import ai.sapper.cdc.core.state.OffsetState;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DFSFileReplicaState extends OffsetState<EFileReplicationState, DFSReplicationOffset> {
    private DFSFileInfo fileInfo;
    private SchemaEntity entity;
    private String zkPath;
    private boolean snapshotReady = false;
    private long snapshotTime;
    private long lastReplicationTime;
    private long updateTime = 0;
    private long recordCount = 0;

    private EFileState fileState = EFileState.Unknown;
    private Map<String, String> storagePath;
    private Map<Long, DFSReplicationDelta> replicationDeltas;

    private List<DFSBlockReplicaState> blocks = new ArrayList<>();

    public DFSFileReplicaState() {
        super(EFileReplicationState.Error, EFileReplicationState.Unknown);
    }

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
        return (fileState == EFileState.New || fileState == EFileState.Updating);
    }

    public void clear() {
        // blocks.clear();
    }
}
