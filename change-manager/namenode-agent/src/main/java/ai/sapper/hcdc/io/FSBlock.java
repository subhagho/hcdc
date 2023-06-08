package ai.sapper.hcdc.io;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.DirectoryInode;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class FSBlock implements Closeable {
    public static final String EXT_BLOCK_FILE = "blk";

    private final FileSystem fs;
    private final DirectoryInode directory;
    private final long blockId;
    private final long previousBlockId;
    private final String filename;
    private FileInode path;
    private long size = -1;
    @Getter(AccessLevel.NONE)
    private Writer writer = null;
    @Getter(AccessLevel.NONE)
    private Reader reader = null;

    public FSBlock(@NonNull DirectoryInode directory,
                   long blockId,
                   long previousBlockId,
                   @NonNull FileSystem fs,
                   String domain) throws IOException {
        this.fs = fs;
        this.directory = directory;
        this.blockId = blockId;
        this.previousBlockId = previousBlockId;
        filename = blockFile(blockId, previousBlockId);
        setup(filename, blockId, false);
    }

    public FSBlock(@NonNull DFSBlockState blockState,
                   @NonNull DirectoryInode directory,
                   @NonNull FileSystem fs,
                   String domain,
                   boolean create) throws IOException {
        this.directory = directory;
        this.fs = fs;
        this.blockId = blockState.getBlockId();
        this.previousBlockId = blockState.getPrevBlockId();
        filename = blockFile(blockState.getBlockId(), blockState.getPrevBlockId());
        setup(filename, blockState.getBlockId(), create);
    }


    private String blockFile(long blockId, long previousBlockId) {
        String p = String.valueOf(previousBlockId);
        if (previousBlockId < 0) p = "NULL";
        return String.format("%d-%s.%s", blockId, p, EXT_BLOCK_FILE);
    }

    private void setup(String path,
                       long blockId,
                       boolean create) throws IOException {
        this.path = (FileInode) fs.getInode(directory().getDomain(), path);
        if (this.path == null) {
            if (!create) {
                throw new IOException(String.format("Block File not found. [block ID=%d][path=%s]",
                        blockId,
                        path));
            }
            this.path = fs.create(directory, path);
            write(new byte[0]);
            close();
        }
    }

    public long size() throws IOException {
        if (size < 0) {
            if (!fs.exists(path.getPathInfo())) {
                size = 0;
            } else {
                size = path.size();
            }
        }
        return size;
    }

    public synchronized void reset() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    public synchronized void seek(int position) throws IOException {
        if (reader == null) {
            reader = fs.reader(path).open();
        }
        reader.seek(position);
    }

    public synchronized long read(byte[] data, int offset, int length) throws IOException {
        Preconditions.checkNotNull(data);
        if (reader == null) {
            reader = fs.reader(path).open();
        }
        return reader.read(data, offset, length);
    }

    public long read(byte[] data) throws IOException {
        return read(data, 0, data.length);
    }

    public synchronized void write(byte[] data, int offset, int length) throws IOException {
        Preconditions.checkNotNull(data);
        if (writer == null) {
            writer = fs.writer(path, true).open();
        }
        writer.write(data, offset, length);
    }

    public void write(byte[] data) throws IOException {
        write(data, 0, data.length);
    }

    public synchronized void append(byte[] data, int offset, int length) throws IOException {
        Preconditions.checkNotNull(data);
        if (writer == null) {
            writer = fs.writer(path, false).open();
        }
        writer.write(data, offset, length);
    }

    public void append(byte[] data) throws IOException {
        append(data, 0, data.length);
    }

    public long truncate(int offset, int length) throws IOException {
        if (writer == null) {
            writer = fs.writer(path, false).open();
        }
        return writer.truncate(offset, length);
    }

    public long truncate(int length) throws IOException {
        if (writer == null) {
            writer = fs.writer(path, false).open();
        }
        return writer.truncate(length);
    }

    public boolean exists() throws IOException {
        return fs.exists(path.getPathInfo());
    }

    protected boolean delete() throws IOException {
        return fs.delete(path.getPathInfo());
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (reader != null) {
                reader.close();
                reader = null;
            }
            if (writer != null) {
                writer.flush();
                writer.close();
                writer = null;
            }
        }
    }
}
