package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.impl.local.LocalPathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class S3PathInfo extends LocalPathInfo {
    private String bucket;

    protected S3PathInfo(@NonNull String bucket, @NonNull String path) {
        super(path);
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
}
