package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.local.LocalWriter;
import lombok.NonNull;

import java.io.IOException;

public class S3Writer extends LocalWriter {
    private final S3FileSystem fs;

    protected S3Writer(@NonNull PathInfo path, @NonNull S3FileSystem fs) {
        super(path);
        this.fs = fs;
    }

    /**
     * @param overwrite
     * @throws IOException
     */
    @Override
    public Writer open(boolean overwrite) throws IOException {
        S3PathInfo s3path = S3FileSystem.checkPath(path());
        if (!overwrite && s3path.exists()) {
            fs.read(s3path);
        }
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
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", path().toString()));
        }
        return super.write(data, offset, length);
    }

    /**
     * @throws IOException
     */
    @Override
    public void flush() throws IOException {
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", path().toString()));
        }
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
        if (!isOpen()) {
            throw new IOException(String.format("Writer not open: [path=%s]", path().toString()));
        }
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
        if (isOpen()) {
            super.close();
            try {
                S3PathInfo s3path = S3FileSystem.checkPath(path());
                fs.upload(s3path.temp(), s3path.parentPathInfo());
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        } else {
            throw new IOException(String.format("Writer not open: [path=%s]", path().toString()));
        }
    }
}
