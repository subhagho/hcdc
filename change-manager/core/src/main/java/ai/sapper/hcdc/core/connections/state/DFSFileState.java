package ai.sapper.hcdc.core.connections.state;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class DFSFileState {
    private String zkPath;
    private String filePath;
    private long createdTime;
    private long updatedTime;
    private long numBlocks;
    private long blockSize;
    private long dataLength;
    private long lastTnxId;
    private boolean deleted = false;
    private final List<DFSBlockState> blocks = new ArrayList<>();
}
