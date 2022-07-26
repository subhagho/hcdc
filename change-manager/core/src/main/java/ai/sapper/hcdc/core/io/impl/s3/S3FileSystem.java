package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.Reader;
import ai.sapper.hcdc.core.io.Writer;
import ai.sapper.hcdc.core.io.impl.local.LocalFileSystem;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3FileSystem extends LocalFileSystem {
    private S3Client client;
    private String defaultBucket;
    private Map<String, String> bucketMap = new HashMap<>();

    private String findBucket(String domain) {
        if (bucketMap.containsKey(domain)) {
            return bucketMap.get(domain);
        }
        return defaultBucket;
    }

    /**
     * @param config
     * @param pathPrefix
     * @return
     * @throws IOException
     */
    @Override
    public FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config, String pathPrefix) throws IOException {
        super.init(config, pathPrefix);
        return this;
    }

    /**
     * @param path
     * @param domain
     * @return
     * @throws IOException
     */
    @Override
    public PathInfo get(@NonNull String path, String domain) throws IOException {
        String bucket = defaultBucket;
        if (bucketMap.containsKey(domain)) {
            bucket = bucketMap.get(domain);
        }
        return new S3PathInfo(domain, bucket, path);
    }

    /**
     * @param config
     * @return
     */
    @Override
    public PathInfo get(@NonNull Map<String, String> config) {
        return new S3PathInfo(config);
    }

    private S3PathInfo checkPath(PathInfo pathInfo) throws IOException {
        if (!(pathInfo instanceof S3PathInfo)) {
            throw new IOException(
                    String.format("Invalid Path type. [type=%s]", pathInfo.getClass().getCanonicalName()));
        }
        return (S3PathInfo) pathInfo;
    }

    /**
     * @param path
     * @param name
     * @return
     * @throws IOException
     */
    @Override
    public String mkdir(@NonNull PathInfo path, @NonNull String name) throws IOException {
        checkPath(path);
        File f = new File(PathUtils.formatPath(String.format("%s/%s", path.path(), name)));
        return f.getAbsolutePath();
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public String mkdirs(@NonNull PathInfo path) throws IOException {
        checkPath(path);
        File f = new File(PathUtils.formatPath(path.path()));
        return f.getAbsolutePath();
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        return super.delete(path, recursive);
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public List<String> list(@NonNull PathInfo path, boolean recursive) throws IOException {
        return super.list(path, recursive);
    }

    /**
     * @param path
     * @param dirQuery
     * @param fileQuery
     * @return
     * @throws IOException
     */
    @Override
    public List<String> find(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException {
        return super.find(path, dirQuery, fileQuery);
    }

    /**
     * @param path
     * @param dirQuery
     * @param fileQuery
     * @return
     * @throws IOException
     */
    @Override
    public List<String> findFiles(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException {
        return super.findFiles(path, dirQuery, fileQuery);
    }

    /**
     * @param path
     * @param createDir
     * @param overwrite
     * @return
     * @throws IOException
     */
    @Override
    public Writer writer(@NonNull PathInfo path, boolean createDir, boolean overwrite) throws IOException {
        return super.writer(path, createDir, overwrite);
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Reader reader(@NonNull PathInfo path) throws IOException {
        return super.reader(path);
    }

    /**
     * @param path
     * @param domain
     * @param prefix
     * @return
     * @throws IOException
     */
    @Override
    public PathInfo get(@NonNull String path, String domain, boolean prefix) throws IOException {
        return super.get(path, domain, prefix);
    }

    /**
     * @param source
     * @param directory
     * @throws IOException
     */
    @Override
    public PathInfo upload(@NonNull File source, @NonNull PathInfo directory) throws IOException {
        return super.upload(source, directory);
    }
}
