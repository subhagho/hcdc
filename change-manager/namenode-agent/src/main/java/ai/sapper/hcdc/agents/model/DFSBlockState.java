package ai.sapper.hcdc.agents.model;

import ai.sapper.cdc.core.model.BlockTransactionDelta;
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

    private List<BlockTransactionDelta> transactions;

    public DFSBlockState add(@NonNull BlockTransactionDelta transaction) {
        if (transactions == null)
            transactions = new ArrayList<>();
        transactions.add(transaction);
        return this;
    }

    public boolean hasTransaction(long txId) {
        if (transactions != null) {
            for (BlockTransactionDelta delta : transactions) {
                if (delta.getTnxId() == txId) return true;
            }
        }
        return false;
    }

    public BlockTransactionDelta delta(long tnxId) {
        if (tnxId <= lastTnxId && transactions != null) {
            for (BlockTransactionDelta delta : transactions) {
                if (delta.getTnxId() == tnxId) return delta;
            }
        }
        return null;
    }

    public List<BlockTransactionDelta> changeSet(long startTxnId) {
        return changeSet(startTxnId, Long.MAX_VALUE);
    }

    public List<BlockTransactionDelta> changeSet(long startTxnId, long endTxnId) {
        if (transactions != null) {
            if (endTxnId > lastTnxId) endTxnId = lastTnxId;
            List<BlockTransactionDelta> set = new ArrayList<>();
            for (BlockTransactionDelta delta : transactions) {
                if (delta.getTnxId() >= startTxnId && delta.getTnxId() <= endTxnId) {
                    set.add(delta);
                }
            }
            if (!set.isEmpty()) return set;
        }
        return null;
    }

    public BlockTransactionDelta compressedChangeSet(long startTnxId) {
        return compressedChangeSet(startTnxId, Long.MAX_VALUE);
    }

    public BlockTransactionDelta compressedChangeSet(long startTnxId, long endTxnId) {
        List<BlockTransactionDelta> deltas = changeSet(startTnxId, endTxnId);
        if (deltas != null && !deltas.isEmpty()) {
            BlockTransactionDelta delta = new BlockTransactionDelta();
            delta.setTnxId(lastTnxId);
            delta.setTimestamp(updatedTime);
            boolean truncate = false;
            for (BlockTransactionDelta d : deltas) {
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
                } else {
                    delta.setEndOffset(d.getEndOffset());
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
