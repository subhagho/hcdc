package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.impl.local.LocalPathInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Getter
@Accessors(fluent = true)
public class S3PathInfo extends LocalPathInfo {
    public static final String CONFIG_KEY_BUCKET = "bucket";

    private final S3Client client;

    private final String bucket;
    private final File temp;

    protected S3PathInfo(@NonNull S3Client client,
                         @NonNull String domain,
                         @NonNull String bucket,
                         @NonNull String path) {
        super(path, domain);
        this.client = client;
        this.bucket = bucket;
        String tempf = String.format("%s/%s.tmp",
                S3FileSystem.TEMP_PATH,
                UUID.randomUUID().toString());
        temp = new File(tempf);
    }

    protected S3PathInfo(@NonNull S3Client client, @NonNull Map<String, String> config) {
        super(config);
        this.client = client;
        bucket = config.get(CONFIG_KEY_BUCKET);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(bucket));
        String tempf = String.format("%s/%s.tmp",
                S3FileSystem.TEMP_PATH,
                UUID.randomUUID().toString());
        temp = new File(tempf);
    }

    /**
     * @return
     */
    @Override
    public PathInfo parentPathInfo() {
        return super.parentPathInfo();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean isDirectory() throws IOException {
        return true;
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean isFile() throws IOException {
        return exists();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean exists() throws IOException {
        try {
            ListObjectsRequest request = ListObjectsRequest
                    .builder()
                    .bucket(bucket())
                    .prefix(path())
                    .build();

            ListObjectsResponse res = client.listObjects(request);
            List<S3Object> objects = res.contents();
            if (objects != null && !objects.isEmpty())
                return true;
        } catch (Exception ex) {
            return false;
        }
        return true;
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public long size() throws IOException {
        return 0L;
    }

    /**
     * @return
     */
    @Override
    public Map<String, String> pathConfig() {
        Map<String, String> config = super.pathConfig();
        config.put(CONFIG_KEY_BUCKET, bucket);
        return config;
    }
}
