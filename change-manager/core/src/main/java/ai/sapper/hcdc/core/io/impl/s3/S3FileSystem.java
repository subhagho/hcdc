package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.Reader;
import ai.sapper.hcdc.core.io.Writer;
import ai.sapper.hcdc.core.io.impl.local.LocalFileSystem;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import software.amazon.awssdk.services.s3.S3Client;

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
        return super.get(path, domain);
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    protected PathInfo get(@NonNull String path) throws IOException {
        return super.get(path);
    }

    /**
     * @param path
     * @param name
     * @return
     * @throws IOException
     */
    @Override
    public String mkdir(@NonNull PathInfo path, @NonNull String name) throws IOException {
        return super.mkdir(path, name);
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public String mkdirs(@NonNull PathInfo path) throws IOException {
        return super.mkdirs(path);
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
}
