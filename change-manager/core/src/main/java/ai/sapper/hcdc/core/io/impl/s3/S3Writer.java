package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.Writer;
import ai.sapper.hcdc.core.io.impl.local.LocalWriter;
import lombok.NonNull;

import java.io.IOException;

public class S3Writer extends LocalWriter {
    protected S3Writer(@NonNull PathInfo path) {
        super(path);
    }

    /**
     * @param overwrite
     * @throws IOException
     */
    @Override
    public Writer open(boolean overwrite) throws IOException {
        return super.open(overwrite);
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
        return super.write(data, offset, length);
    }

    /**
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        super.flush();
    }

    /**
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public long truncate(long offset, long length) throws IOException {
        return super.truncate(offset, length);
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
        super.close();
    }
}
