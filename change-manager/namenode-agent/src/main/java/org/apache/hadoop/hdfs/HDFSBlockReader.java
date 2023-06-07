package org.apache.hadoop.hdfs;

import ai.sapper.cdc.common.cache.LRUCache;
import ai.sapper.cdc.core.io.EncryptionHandler;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.hcdc.io.EncryptionContext;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.hadoop.hdfs.protocol.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Getter
@Accessors(fluent = true)
public class HDFSBlockReader extends DFSInputStream {
    private static final int DEFAULT_CACHE_SIZE = 32;

    private HdfsFileStatus fileInfo;
    private LocatedBlocks locatedBlocks = null;
    private final LRUCache<Long, HDFSBlockData> cache = new LRUCache<>(DEFAULT_CACHE_SIZE);
    private EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler;

    public HDFSBlockReader(@NonNull DFSClient client, @NonNull String path) throws IOException {
        super(client, path, true, null);
    }

    public HDFSBlockReader init(EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler) throws DFSError {
        checkOpen();
        this.encryptionHandler = encryptionHandler;
        try {
            fileInfo = dfsClient.getFileInfo(src);
            if (fileInfo == null) {
                throw new IOException(String.format("File not found. [path=%s]", src));
            }
            locatedBlocks = dfsClient.getLocatedBlocks(src, 0, fileInfo.getLen());
            return this;
        } catch (Exception ex) {
            throw new DFSError(ex);
        }
    }

    private LocatedBlock findBlock(long blockId) {
        Preconditions.checkNotNull(locatedBlocks);
        for (LocatedBlock block : locatedBlocks.getLocatedBlocks()) {
            ExtendedBlock eb = block.getBlock();
            if (eb.getBlockId() == blockId) {
                return block;
            }
        }
        return null;
    }

    public HDFSBlockData read(long blockId, long generationStamp, long offset, int length) throws DFSError {
        checkOpen();

        if (cache.containsKey(blockId)) {
            return cache.get(blockId).get();
        }
        LocatedBlock lb = findBlock(blockId);
        if (lb == null) {
            throw new DFSError(String.format("Block not found. [path=%s][block id=%d]", src, blockId));
        }
        ExtendedBlock block = lb.getBlock();
        if (generationStamp <= block.getGenerationStamp()) {
            if (length < 0) {
                length = (int) (block.getNumBytes() - offset);
            } else {
                length = (int) (offset + length > block.getNumBytes() ? (block.getNumBytes() - offset) : length);
            }

            HDFSBlockData data = new HDFSBlockData();
            data.path(src);
            data.blockId(blockId);
            data.name(block.getBlockName());
            data.blockPoolId(block.getBlockPoolId());
            data.generationStamp(block.getGenerationStamp());
            data.dataSize(block.getNumBytes());
            data.blockSize(fileInfo.getBlockSize());
            data.offset(offset);
            data.length(length);

            byte[] da = new byte[length];
            ByteBuffer buffer = ByteBuffer.wrap(da);
            Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap = new HashMap<>();
            try {
                fetchBlockByteRange(lb, offset, (offset + length - 1), buffer, corruptedBlockMap);
                buffer.rewind();
                if (encryptionHandler != null) {
                    EncryptionContext ctx = new EncryptionContext();
                    ctx.put(EncryptionContext.CONTEXT_FILE_PATH, data.path());
                    ctx.put(EncryptionContext.CONTEXT_BLOCK_ID, data.blockId());
                    ctx.put(EncryptionContext.CONTEXT_BLOCK_NAME, data.name());

                    ByteBuffer db = encryptionHandler.decrypt(buffer, ctx);
                    if (db == null) {
                        throw new IOException(
                                String.format("[block=%d] Failed to decrypt data. [path=%s]",
                                        data.blockId(), data.path()));
                    }
                    db.rewind();
                    buffer = db;
                }
                data.data(buffer);
            } catch (IOException e) {
                throw new DFSError(e);
            } finally {
                reportCheckSumFailure(corruptedBlockMap, lb.getLocations().length);
            }
            cache.put(blockId, data);
            return data;
        }
        return null;
    }

    private void checkOpen() throws DFSError {
        if (!dfsClient.isClientRunning()) {
            throw new DFSError("Client is not running.");
        }
    }

    /**
     * Close it down!
     */
    @Override
    public synchronized void close() throws IOException {
        cache.clear();
        super.close();
    }
}
