package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.impl.local.LocalPathInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class S3PathInfo extends LocalPathInfo {
    public static final String CONFIG_KEY_BUCKET = "bucket";

    private final S3Client client;
    private final String bucket;
    private File temp;

    protected S3PathInfo(@NonNull S3Client client,
                         @NonNull String domain,
                         @NonNull String bucket,
                         @NonNull String path) {
        super(path, domain);
        this.client = client;
        this.bucket = bucket;
        String fname = FilenameUtils.getName(path());
        if (!Strings.isNullOrEmpty(fname)) {
            String tempf = String.format("%s/%s",
                    S3FileSystem.TEMP_PATH,
                    fname);
            temp = new File(tempf);
        }
        file(temp);
    }

    protected S3PathInfo(@NonNull S3Client client, @NonNull Map<String, String> config) {
        super(config);
        this.client = client;
        bucket = config.get(CONFIG_KEY_BUCKET);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(bucket));
        String fname = FilenameUtils.getName(path());
        if (!Strings.isNullOrEmpty(fname)) {
            String tempf = String.format("%s/%s",
                    S3FileSystem.TEMP_PATH,
                    fname);
            temp = new File(tempf);
        }
        file(temp);
    }

    /**
     * @return
     */
    @Override
    public PathInfo parentPathInfo() {
        String path = path();
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 2);
        }
        String p = FilenameUtils.getFullPath(path);
        return new S3PathInfo(client, domain(), bucket, p);
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean isDirectory() throws IOException {
        return path().endsWith("/");
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
        if (path().endsWith("/")) return true;
        try {
            ListObjectsRequest request = ListObjectsRequest
                    .builder()
                    .bucket(bucket())
                    .prefix(parent())
                    .build();
            ListObjectsResponse res = client.listObjects(request);
            List<S3Object> objects = res.contents();
            if (objects != null && !objects.isEmpty()) {
                for (S3Object so : objects) {
                    if (so.key().compareTo(path()) == 0) {
                        return true;
                    }
                }
            }
            return false;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public long size() throws IOException {
        if (temp.exists()) {
            Path p = Paths.get(temp.toURI());
            dataSize(Files.size(p));
        } else {
            dataSize(0);
        }
        return dataSize();
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

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return "{bucket=" + bucket + ", path=" + path() + "[domain=" + domain() + "]}";
    }
}
