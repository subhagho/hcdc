package ai.sapper.hcdc.core.connections.state;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import net.minidev.json.annotate.JsonIgnore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
@Setter
public class DFSFileState {
    private String zkPath;
    private String hdfsFilePath;
    private long createdTime;
    private long updatedTime;
    private long numBlocks;
    private long blockSize;
    private long dataLength;
    private long lastTnxId;
    private long timestamp;
    private boolean deleted = false;
    private List<DFSBlockState> blocks;

    public DFSFileState add(@NonNull DFSBlockState block) {
        if (blocks == null)
            blocks = new ArrayList<>();
        blocks.add(block);

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

    @JsonIgnore
    public List<DFSBlockState> getSorted() {
        if (blocks != null && !blocks.isEmpty()) {
            blocks.sort(new DFSBlockComparator());
        }
        return blocks;
    }
}
