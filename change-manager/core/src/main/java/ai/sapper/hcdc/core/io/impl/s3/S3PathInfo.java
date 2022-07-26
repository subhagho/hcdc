package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.impl.local.LocalPathInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class S3PathInfo extends LocalPathInfo {
    public static final String CONFIG_KEY_BUCKET = "bucket";

    private final String bucket;

    protected S3PathInfo(@NonNull String domain, @NonNull String bucket, @NonNull String path) {
        super(path, domain);
        this.bucket = bucket;
    }

    protected S3PathInfo(@NonNull Map<String, String> config) {
        super(config);
        bucket = config.get(CONFIG_KEY_BUCKET);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(bucket));
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
        return super.isDirectory();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean isFile() throws IOException {
        return super.isFile();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean exists() throws IOException {
        return super.exists();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public long size() throws IOException {
        return super.size();
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
