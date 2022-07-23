package ai.sapper.hcdc.core.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class DFSBlockState {
    private long blockId;
    private long prevBlockId = -1;
    private long createdTime;
    private long updatedTime;
    private long dataSize = 0;
    private long blockSize;
    private long lastTnxId;
    private long generationStamp;
    private String blockChecksum;
    private boolean stored = false;

    private EBlockState state = EBlockState.Unknown;

    private List<BlockTnxDelta> transactions;

    public DFSBlockState add(@NonNull BlockTnxDelta transaction) {
        if (transactions == null)
            transactions = new ArrayList<>();
        transactions.add(transaction);
        return this;
    }

    public BlockTnxDelta delta(long tnxId) {
        if (tnxId <= lastTnxId && transactions != null) {
            for (BlockTnxDelta delta : transactions) {
                if (delta.getTnxId() == tnxId) return delta;
            }
        }
        return null;
    }

    public List<BlockTnxDelta> changeSet(long tnxId) {
        if (tnxId <= lastTnxId && transactions != null) {
            List<BlockTnxDelta> set = new ArrayList<>();
            for (BlockTnxDelta delta : transactions) {
                if (delta.getTnxId() >= tnxId) {
                    set.add(delta);
                }
            }
            if (!set.isEmpty()) return set;
        }
        return null;
    }

    public BlockTnxDelta compressedChangeSet(long tnxId) {
        List<BlockTnxDelta> deltas = changeSet(tnxId);
        if (deltas != null && !deltas.isEmpty()) {
            BlockTnxDelta delta = new BlockTnxDelta();
            delta.setTnxId(lastTnxId);
            delta.setTimestamp(updatedTime);
            boolean truncate = false;
            for (BlockTnxDelta d : deltas) {
                if (d.isDeleted()) {
                    delta.setDeleted(true);
                    break;
                }
                if (d.getStartOffset() < delta.getStartOffset()) {
                    truncate = true;
                }
                delta.setStartOffset(d.getStartOffset());
                if (d.getEndOffset() < delta.getEndOffset()) {
                    if (truncate) {
                        delta.setEndOffset(d.getEndOffset());
                        truncate = false;
                    } else {
                        throw new RuntimeException(
                                String.format("Invalid Block state: Offset out of sync. [block ID=%d]", blockId));
                    }
                }
            }
            return delta;
        }
        return null;
    }

    public boolean hasTransactions() {
        return (transactions != null && !transactions.isEmpty());
    }

    public boolean blockIsFull() {
        return (dataSize == blockSize);
    }

    public boolean canUpdate() {
        return (state == EBlockState.New || state == EBlockState.Updating);
    }
}
