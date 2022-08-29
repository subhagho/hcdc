package ai.sapper.hcdc.io;

import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Getter
@Accessors(fluent = true)
public class FSFile implements Closeable {
    private final String domain;
    private final FileSystem fs;
    private final PathInfo directory;
    @Getter(AccessLevel.NONE)
    private final List<FSBlock> blocks = new ArrayList<>();
    @Getter(AccessLevel.NONE)
    private int currentIndex = 0;

    public FSFile(@NonNull String domain,
                  @NonNull PathInfo directory,
                  @NonNull FileSystem fs) {
        this.domain = domain;
        this.directory = directory;
        this.fs = fs;
    }

    public FSFile(@NonNull DFSFileState fileState,
                  String domain,
                  @NonNull FileSystem fs,
                  boolean create) throws IOException {
        this.domain = domain;
        this.fs = fs;
        String p = PathUtils.formatPath(String.format("%s/", fileState.getFileInfo().getHdfsPath()));
        directory = fs.get(p, domain);
        setup(create, fileState);
    }

    public FSFile(@NonNull DFSFileState fileState,
                  String domain,
                  @NonNull FileSystem fs) throws IOException {
        this.domain = domain;
        this.fs = fs;
        String p = PathUtils.formatPath(String.format("%s/", fileState.getFileInfo().getHdfsPath()));
        directory = fs.get(p, domain);
        setup(false, fileState);
    }

    private void setup(boolean create, DFSFileState fileState) throws IOException {
        if (!directory.exists()) {
            if (!create) {
                throw new IOException(String.format("FS File does not exist. [path=%s]", directory.path()));
            }
            fs.mkdirs(directory);
        } else {
            if (!directory.isDirectory()) {
                throw new IOException(
                        String.format("Specified path already exists and not a directory. [path=%s]",
                                directory.path()));
            }
        }
        if (fileState.hasBlocks()) {
            for (DFSBlockState blockState : fileState.getBlocks()) {
                FSBlock block = new FSBlock(blockState, directory, fs, domain, create);
                blocks.add(block);
            }
        }
    }

    public FSBlock get(long blockId) {
        for (FSBlock block : blocks) {
            if (block.blockId() == blockId) return block;
        }
        return null;
    }

    public boolean hasBlocks() {
        return (!blocks.isEmpty());
    }

    public Collection<FSBlock> getBlocksList() {
        return blocks;
    }

    public void open(boolean overwrite) throws IOException {
        if (!directory.exists()) {
            fs.mkdirs(directory);
        }
    }

    public synchronized void reset() throws IOException {
        currentIndex = 0;
        if (!blocks.isEmpty()) {
            for (FSBlock block : blocks) {
                block.reset();
            }
        }
    }

    public synchronized boolean seek(int position) throws IOException {
        int offset = position;
        if (!blocks.isEmpty()) {
            currentIndex = 0;
            while (offset > 0 && currentIndex < blocks.size()) {
                FSBlock block = blocks.get(currentIndex);
                int size = (int) block.size();
                if (size > offset) {
                    block.seek(offset);
                    break;
                }
                if (size == offset) {
                    currentIndex++;
                    break;
                }
                currentIndex++;
                offset -= size;
            }
        }
        return (offset == 0);
    }

    public synchronized long read(byte[] data, int offset, int length) throws IOException {
        if (blocks.isEmpty()) return -1;
        long r = 0;
        int off = offset;
        while (r < length && currentIndex < blocks.size()) {
            FSBlock block = blocks.get(currentIndex);
            long rr = block.read(data, off, (int) (length - r));
            if (rr < (length - r)) {
                currentIndex++;
            }
            r += rr;
            off += rr;
        }
        return r;
    }

    public synchronized FSBlock add(long blockId, long previousBlockId) throws IOException {
        FSBlock block = get(blockId);
        if (block == null) {
            if (previousBlockId < 0) {
                if (!blocks.isEmpty()) {
                    throw new IOException("Invalid block chain: Blocks already added.");
                }
            } else {
                if (blocks.isEmpty()) {
                    throw new IOException("Invalid block chain: Blocks is empty.");
                }
                FSBlock prev = blocks.get(blocks.size() - 1);
                if (prev.blockId() != previousBlockId) {
                    throw new IOException(
                            String.format("Invalid block chain: Previous block ID expected = %d", prev.blockId()));
                }
            }
            block = new FSBlock(directory, blockId, previousBlockId, fs, domain);
            blocks.add(block);
        }
        return block;
    }

    public synchronized FSBlock add(@NonNull DFSBlockState blockState) throws IOException {
        FSBlock block = get(blockState.getBlockId());
        if (block == null) {
            if (blockState.getPrevBlockId() < 0) {
                if (!blocks.isEmpty()) {
                    throw new IOException("Invalid block chain: Blocks already added.");
                }
            } else {
                if (blocks.isEmpty()) {
                    throw new IOException("Invalid block chain: Blocks is empty.");
                }
                FSBlock prev = blocks.get(blocks.size() - 1);
                if (prev.blockId() != blockState.getPrevBlockId()) {
                    throw new IOException(
                            String.format("Invalid block chain: Previous block ID expected = %d", prev.blockId()));
                }
            }
            block = new FSBlock(blockState, directory, fs, domain, true);
            blocks.add(block);
        }
        return block;
    }

    public synchronized boolean remove(long blockId) throws IOException {
        FSBlock block = deleteBlock(blockId);
        if (block != null) {
            return block.delete();
        }
        return false;
    }

    private FSBlock deleteBlock(long blockId) {
        int indx = -1;
        for (int ii = 0; ii < blocks.size(); ii++) {
            FSBlock block = blocks.get(ii);
            if (block.blockId() == blockId) {
                indx = ii;
                break;
            }
        }
        if (indx >= 0) {
            FSBlock block = blocks.remove(indx);
            if (block != null) {
                return block;
            }
        }
        return null;
    }

    public boolean exists() throws IOException {
        return directory.exists();
    }

    public synchronized int delete() throws IOException {
        int count = 0;
        if (hasBlocks()) {
            for (FSBlock block : blocks) {
                if (block.delete()) {
                    count++;
                }
            }
            blocks.clear();
        }
        fs.delete(directory);
        return count;
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
            if (!blocks.isEmpty()) {
                for (FSBlock block : blocks) {
                    block.close();
                }
            }
            blocks.clear();
        }
    }

    public File archive() throws IOException {
        if (hasBlocks()) {
            File dir = PathUtils.getTempDir();
            for (FSBlock block : blocks) {
                int bsize = 8095;
                byte[] buffer = new byte[bsize];
                File bf = new File(String.format("%s/%s.blk", dir.getAbsolutePath(), block.blockId()));
                try (FileOutputStream fos = new FileOutputStream(bf)) {
                    while (true) {
                        long r = block.read(buffer);
                        if (r > 0)
                            fos.write(buffer, 0, (int) r);
                        if (r < bsize) break;
                    }
                }
            }
            File zip = PathUtils.getTempFileWithExt("zip");
            Path p = Files.createFile(Paths.get(zip.getAbsolutePath()));
            try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(p))) {
                Path pp = Paths.get(dir.getAbsolutePath());
                Files.walk(pp)
                        .filter(path -> !Files.isDirectory(path))
                        .forEach(path -> {
                            ZipEntry zipEntry = new ZipEntry(pp.relativize(path).toString());
                            try {
                                zs.putNextEntry(zipEntry);
                                Files.copy(path, zs);
                                zs.closeEntry();
                            } catch (IOException e) {
                                System.err.println(e);
                            }
                        });
            }
            return zip;
        }
        return null;
    }
}
