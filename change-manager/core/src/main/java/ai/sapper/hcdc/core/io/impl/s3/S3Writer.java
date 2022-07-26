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
    private final S3Client client;

    protected S3Writer(@NonNull PathInfo path, @NonNull S3Client client) {
        super(path);
        this.client = client;
    }

    /**
     * @param overwrite
     * @throws IOException
     */
    @Override
    public Writer open(boolean overwrite) throws IOException {
        S3PathInfo s3path = S3FileSystem.checkPath(path());
        if (!overwrite && s3path.exists()) {
            download(s3path);
            FileOutputStream fos = new FileOutputStream(s3path.file(), true);
            outputStream(fos);
        } else {
            FileOutputStream fos = new FileOutputStream(s3path.file());
            outputStream(fos);
        }
        return this;
    }

    private void download(S3PathInfo path) throws IOException {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(path.bucket())
                .key(path.path())
                .build();
        client.getObject(request, ResponseTransformer.toFile(path.file()));
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
        S3PathInfo s3path = S3FileSystem.checkPath(path());
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(s3path.bucket())
                .key(s3path.path())
                .build();
        PutObjectResponse response = client.putObject(request, RequestBody.fromFile(s3path.file()));
        S3Waiter waiter = client.waiter();
        HeadObjectRequest requestWait = HeadObjectRequest.builder()
                .bucket(s3path.bucket())
                .key(s3path.path())
                .build();

        WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);

        waiterResponse.matched().response().ifPresent(System.out::println);
    }
}
