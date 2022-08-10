package ai.sapper.hcdc.core.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class DFSFileState {
    private long id;
    private String zkPath;
    private String hdfsFilePath;
    private long createdTime;
    private long updatedTime;
    private long numBlocks;
    private long blockSize;
    private long dataSize;
    private long lastTnxId;
    private long timestamp;
    private String schemaLocation;
    private EFileType fileType = EFileType.UNKNOWN;
    private EFileState state = EFileState.Unknown;

    private List<DFSBlockState> blocks;

    public DFSFileState add(@NonNull DFSBlockState block) {
        if (blocks == null)
            blocks = new ArrayList<>();
        blocks.add(block);
        numBlocks++;

        return this;
    }

    public DFSBlockState get(long blockId) {
        if (blocks != null && !blocks.isEmpty()) {
            for (DFSBlockState bs : blocks) {
                if (bs.getBlockId() == blockId) {
                    return bs;
                }
            }
        }
        return null;
    }

    public List<DFSBlockState> sortedBlocks() {
        if (blocks != null && !blocks.isEmpty()) {
            blocks.sort(new DFSBlockComparator());
        }
        return blocks;
    }

    public Map<Long, BlockTransactionDelta> compressedChangeSet(long startTxnId, long endTxnId) {
        if (blocks != null && !blocks.isEmpty()) {
            Map<Long, BlockTransactionDelta> change = new HashMap<>();
            for (DFSBlockState block : sortedBlocks()) {
                BlockTransactionDelta c = block.compressedChangeSet(startTxnId, endTxnId);
                if (c != null) {
                    change.put(block.getBlockId(), c);
                }
            }
            if (!change.isEmpty()) return change;
        }
        return null;
    }

    public DFSBlockState findFirstBlock() {
        if (blocks != null && !blocks.isEmpty()) {
            return blocks.get(0);
        }
        return null;
    }

    public DFSBlockState findLastBlock() {
        if (blocks != null && !blocks.isEmpty()) {
            return blocks.get(blocks.size() - 1);
        }
        return null;
    }

    public boolean checkDeleted() {
        return (state == EFileState.Deleted);
    }

    public boolean hasError() {
        return (state == EFileState.Error);
    }

    public boolean canProcess() {
        return (!hasError() && !checkDeleted());
    }

    public boolean hasBlocks() {
        return (blocks != null && !blocks.isEmpty());
    }
}
