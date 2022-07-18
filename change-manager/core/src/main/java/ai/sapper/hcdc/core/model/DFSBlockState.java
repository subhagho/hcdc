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
    private EBlockState state = EBlockState.Unknown;

    private List<BlockTnxDelta> transactions;

    public DFSBlockState add(@NonNull BlockTnxDelta transaction) {
        if (transactions == null)
            transactions = new ArrayList<>();
        transactions.add(transaction);
        return this;
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
