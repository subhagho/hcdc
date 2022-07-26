package ai.sapper.hcdc.core.io.impl.local;

import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.Writer;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

@Getter
@Setter
@Accessors(fluent = true)
public class LocalWriter extends Writer {
    private FileOutputStream outputStream;

    protected LocalWriter(@NonNull PathInfo path) {
        super(path);
        Preconditions.checkArgument(path instanceof LocalPathInfo);
    }

    /**
     * @param overwrite
     * @throws IOException
     */
    @Override
    public Writer open(boolean overwrite) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) path();
        if (!pi.exists() || overwrite) {
            outputStream = new FileOutputStream(pi.file());
        } else if (pi.exists()) {
            outputStream = new FileOutputStream(pi.file(), true);
        }
        return this;
    }

    /**
     * @param data
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public long write(byte[] data, long offset, long length) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) path();
        if (outputStream == null) {
            throw new IOException(String.format("File Stream not open. [path=%s]", pi.file().getAbsolutePath()));
        }
        outputStream.write(data, (int) offset, (int) length);

        return length;
    }

    /**
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        LocalPathInfo pi = (LocalPathInfo) path();
        if (outputStream == null) {
            throw new IOException(String.format("File Stream not open. [path=%s]", pi.file().getAbsolutePath()));
        }
        outputStream.flush();
    }

    /**
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public long truncate(long offset, long length) throws IOException {
        LocalPathInfo pi = (LocalPathInfo) path();
        if (outputStream == null) {
            throw new IOException(String.format("File Stream not open. [path=%s]", pi.file().getAbsolutePath()));
        }
        FileChannel channel = outputStream.getChannel();
        channel = channel.truncate(offset + length);
        return channel.size();
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
        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
            outputStream = null;
        }
    }
}
