package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.cache.EvictionCallback;
import ai.sapper.cdc.common.cache.LRUCache;
import ai.sapper.cdc.common.utils.DefaultLogger;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class S3ReadCache implements EvictionCallback<String, S3ReadCache.CacheFile> {
    public static final String __CONFIG_PATH = "cache";
    public static final String CONFIG_CACHE_SIZE = "size";
    public static final String CONFIG_CACHE_TIMEOUT = "timeout";

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class CacheFile {
        private String key;
        private File path;
        private long modified = 0;
    }

    private static final int CACHE_SIZE = 128;
    private static final long CACHE_TIMEOUT = 5 * 60 * 1000;

    private final Map<String, LRUCache<String, CacheFile>> cache = new HashMap<>();
    private final S3FileSystem fs;
    private int cacheSize = CACHE_SIZE;
    private long cacheTimeout = CACHE_TIMEOUT;

    public S3ReadCache(@NonNull S3FileSystem fs) {
        this.fs = fs;
    }

    public S3ReadCache init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        if (ConfigReader.checkIfNodeExists(xmlConfig, __CONFIG_PATH)) {
            HierarchicalConfiguration<ImmutableNode> node = xmlConfig.configurationAt(__CONFIG_PATH);
            if (node.containsKey(CONFIG_CACHE_SIZE)) {
                cacheSize = node.getInt(CONFIG_CACHE_SIZE);
            }
            if (node.containsKey(CONFIG_CACHE_TIMEOUT)) {
                cacheTimeout = node.getLong(CONFIG_CACHE_TIMEOUT);
            }
        }
        return this;
    }

    public S3PathInfo get(@NonNull S3PathInfo path) throws Exception {
        LRUCache<String, CacheFile> cache = get(path.bucket());
        if (cache.containsKey(path.path())) {
            Optional<CacheFile> cf = cache.get(path.path());
            if (cf.isPresent()) {
                CacheFile c = cf.get();
                if (c.path != null && c.path.exists()) {
                    long to = System.currentTimeMillis() - c.modified;
                    if (to < cacheTimeout) {
                        return path.withTemp(c.path);
                    }
                    long ut = fs.updateTime(path);
                    if (c.modified >= ut) {
                        DefaultLogger.LOGGER.debug(String.format("Found in cache. [path=%s][file=%s]", c.key, c.path));
                        return path.withTemp(c.path);
                    }
                }
            }
        }
        fs.download(path);

        return path.withTemp(put(path, System.currentTimeMillis()));
    }

    public File put(@NonNull S3PathInfo path, long updated) throws Exception {
        synchronized (cache) {
            LRUCache<String, CacheFile> cache = get(path.bucket());
            Optional<CacheFile> cf = cache.get(path.path());
            if (cf.isPresent()) {
                cf.get().path = path.temp();
                cf.get().modified = updated;
            } else {
                CacheFile c = new CacheFile();
                c.key = path.path();
                c.path = path.temp();
                c.modified = System.currentTimeMillis();
                cache.put(c.key, c);
            }
            return path.temp();
        }
    }

    private LRUCache<String, CacheFile> get(String bucket) {
        synchronized (cache) {
            if (!cache.containsKey(bucket)) {
                cache.put(bucket,
                        new LRUCache<String, CacheFile>(cacheSize)
                                .withEvictionCallback(this));
            }
            return cache.get(bucket);
        }
    }

    @Override
    public void evicted(String key, CacheFile value) {
        if (value.path.exists()) {
            boolean ret = value.path.delete();
            if (ret) {
                DefaultLogger.LOGGER.debug(
                        String.format("Removed cache file. [path=%s]", value.path.getAbsolutePath()));
            } else {
                DefaultLogger.LOGGER.debug(
                        String.format("Failed to remove cache file. [path=%s]", value.path.getAbsolutePath()));
            }
        }
    }
}
