package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.Writer;
import ai.sapper.hcdc.core.io.impl.local.LocalWriter;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.FileOutputStream;
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
            fs.download(s3path);
            FileOutputStream fos = new FileOutputStream(s3path.file(), true);
            outputStream(fos);
        } else {
            FileOutputStream fos = new FileOutputStream(s3path.file());
            outputStream(fos);
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

            S3PathInfo s3path = S3FileSystem.checkPath(path());
            fs.upload(s3path.file(), s3path.parentPathInfo());
        } else {
            throw new IOException(String.format("Writer not open: [path=%s]", path().toString()));
        }
    }
}
