package ai.sapper.hcdc.core.connections.state;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

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
    private boolean deleted = false;

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

    public boolean hasBlocks() {
        return (blocks != null && !blocks.isEmpty());
    }
}
