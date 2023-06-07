package ai.sapper.cdc.core.model;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;

@Getter
@Setter
@Accessors(fluent = true)
public class HDFSBlockData {
    private String path;
    private long blockId;
    private String name;
    private String blockPoolId;
    private long generationStamp;
    private long blockSize;
    private long dataSize;
    private long offset;
    private long length;
    private ByteBuffer data;

    public boolean isComplete() {
        return (dataSize == blockSize);
    }
}
