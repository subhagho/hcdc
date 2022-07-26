package ai.sapper.hcdc.core.io.impl.s3;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.Reader;
import ai.sapper.hcdc.core.io.Writer;
import ai.sapper.hcdc.core.io.impl.local.LocalFileSystem;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3FileSystem extends LocalFileSystem {
    public static final String TEMP_PATH = String.format("%s/HCDC/S3",
            System.getProperty("java.io.tmpdir"));
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
        try {
            S3FileSystemConfig cfg = new S3FileSystemConfig(config, pathPrefix);
            cfg.read();
            fsConfig(cfg);
            super.init(config, pathPrefix);

            this.defaultBucket = cfg.defaultBucket;
            if (cfg.mappings != null) {
                bucketMap = cfg.mappings;
            }
            Region region = Region.of(cfg.region);
            client = S3Client.builder()
                    .region(region)
                    .build();

            File tdir = new File(TEMP_PATH);
            if (!tdir.exists()) {
                tdir.mkdirs();
            }
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
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
        return new S3PathInfo(this.client, domain, bucket, path);
    }

    /**
     * @param config
     * @return
     */
    @Override
    public PathInfo get(@NonNull Map<String, String> config) {
        return new S3PathInfo(this.client, config);
    }

    public static S3PathInfo checkPath(PathInfo pathInfo) throws IOException {
        if (!(pathInfo instanceof S3PathInfo)) {
            throw new IOException(
                    String.format("Invalid Path type. [type=%s]", pathInfo.getClass().getCanonicalName()));
        }
        return (S3PathInfo) pathInfo;
    }

    private boolean bucketExists(String bucket) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucket)
                .build();
        try {
            client.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
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
        S3PathInfo s3path = checkPath(path);
        if (bucketExists(s3path.bucket())) {
            if (recursive) {
                boolean ret = true;
                ListObjectsRequest request = ListObjectsRequest
                        .builder()
                        .bucket(s3path.bucket())
                        .prefix(path.path())
                        .build();

                ListObjectsResponse res = client.listObjects(request);
                List<S3Object> objects = res.contents();
                for (S3Object obj : objects) {
                    DeleteObjectRequest dr = DeleteObjectRequest.builder()
                            .bucket(s3path.bucket())
                            .key(obj.key())
                            .build();
                    DeleteObjectResponse dres = client.deleteObject(dr);
                    if (!dres.deleteMarker() && ret) {
                        ret = false;
                    }
                }
                return ret;
            } else {
                DeleteObjectRequest dr = DeleteObjectRequest.builder()
                        .bucket(s3path.bucket())
                        .key(s3path.path())
                        .build();
                DeleteObjectResponse dres = client.deleteObject(dr);
                return dres.deleteMarker();
            }
        }
        return false;
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public List<String> list(@NonNull PathInfo path, boolean recursive) throws IOException {
        S3PathInfo s3path = checkPath(path);
        if (bucketExists(s3path.bucket())) {
            ListObjectsRequest request = ListObjectsRequest
                    .builder()
                    .bucket(s3path.bucket())
                    .prefix(path.path())
                    .build();

            ListObjectsResponse res = client.listObjects(request);
            List<S3Object> objects = res.contents();
            List<String> paths = new ArrayList<>();
            for (S3Object obj : objects) {
                if (recursive) {
                    paths.add(obj.key());
                } else {
                    String p = obj.key();
                    p = p.replaceFirst(path.path(), "");
                    if (p.startsWith("/")) {
                        p = p.substring(1);
                    }
                    if (p.indexOf('/') < 0) {
                        paths.add(obj.key());
                    }
                }
            }
            if (!paths.isEmpty()) return paths;
        }
        return null;
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
        List<String> paths = list(path, true);
        if (paths != null && !paths.isEmpty()) {
            Pattern dp = null;
            if (!Strings.isNullOrEmpty(dirQuery)) {
                dp = Pattern.compile(dirQuery);
            }
            Pattern fp = Pattern.compile(fileQuery);
            List<String> out = new ArrayList<>();
            for (String p : paths) {
                String fname = FilenameUtils.getName(p);
                String dir = FilenameUtils.getPath(p);
                Matcher dm = null;
                if (dp != null) {
                    dm = dp.matcher(dir);
                }
                Matcher fm = fp.matcher(fname);
                if (dm == null || dm.matches()) {
                    if (fm.matches()) {
                        out.add(p);
                    }
                }
            }
            if (!out.isEmpty()) return out;
        }
        return null;
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
        return find(path, dirQuery, fileQuery);
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

    public static class S3FileSystemConfig extends FileSystemConfig {
        public static final String CONFIG_REGION = "region";
        public static final String CONFIG_DEFAULT_BUCKET = "defaultBucket";
        public static final String CONFIG_DOMAIN_MAP = "domains.mapping";

        private String region;
        private String defaultBucket;
        private Map<String, String> mappings;

        public S3FileSystemConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                  @NonNull String path) {
            super(config, path);
        }

        /**
         * @throws ConfigurationException
         */
        @Override
        public void read() throws ConfigurationException {
            super.read();
            region = get().getString(CONFIG_REGION);
            if (Strings.isNullOrEmpty(region)) {
                throw new ConfigurationException(
                        String.format("S3 File System : missing configuration. [name=%s]", CONFIG_REGION));
            }
            defaultBucket = get().getString(CONFIG_DEFAULT_BUCKET);
            if (Strings.isNullOrEmpty(defaultBucket)) {
                throw new ConfigurationException(
                        String.format("S3 File System : missing configuration. [name=%s]", CONFIG_DEFAULT_BUCKET));
            }
            if (ConfigReader.checkIfNodeExists(get(), CONFIG_DOMAIN_MAP)) {
                mappings = ConfigReader.readAsMap(get(), CONFIG_DOMAIN_MAP);
            }
        }
    }
}
