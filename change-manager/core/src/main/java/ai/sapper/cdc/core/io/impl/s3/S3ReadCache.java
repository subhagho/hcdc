package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.common.cache.EvictionCallback;
import ai.sapper.cdc.common.cache.LRUCache;
import ai.sapper.cdc.common.utils.DefaultLogger;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class S3ReadCache implements EvictionCallback<String, S3ReadCache.CacheFile> {

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class CacheFile {
        private String key;
        private File path;
        private long modified = 0;
    }

    private static final int CACHE_SIZE = 128;
    private final Map<String, LRUCache<String, CacheFile>> cache = new HashMap<>(CACHE_SIZE);
    private final S3FileSystem fs;

    public S3ReadCache(@NonNull S3FileSystem fs) {
        this.fs = fs;
    }

    public S3PathInfo get(@NonNull S3PathInfo path) throws Exception {
        LRUCache<String, CacheFile> cache = get(path.bucket());
        if (cache.containsKey(path.path())) {
            Optional<CacheFile> cf = cache.get(path.path());
            if (cf.isPresent()) {
                CacheFile c = cf.get();
                if (c.path != null && c.path.exists()) {
                    long ut = fs.updateTime(path);
                    if (c.modified >= ut) {
                        DefaultLogger.LOGGER.debug(String.format("Found in cache. [path=%s][file=%s]", c.key, c.path));
                        return path.withTemp(c.path);
                    }
                }
            }
        }
        Optional<CacheFile> cf = get(path, cache);
        if (cf.isPresent()) {
            fs.download(path);
            cf.get().modified = System.currentTimeMillis();
        }
        return path;
    }

    public void put(@NonNull S3PathInfo path, long updated) throws Exception {
        LRUCache<String, CacheFile> cache = get(path.bucket());
        Optional<CacheFile> cf = get(path, cache);
        if (cf.isPresent()) {
            cf.get().path = path.temp();
            cf.get().modified = updated;
        }
    }

    private Optional<CacheFile> get(S3PathInfo path, LRUCache<String, CacheFile> cache) throws Exception {
        synchronized (cache) {
            if (!cache.containsKey(path.path())) {
                CacheFile cf = new CacheFile();
                cf.key = path.path();
                cf.path = path.temp();
                cf.modified = 0;
                cache.put(cf.key, cf);
            }
            return cache.get(path.path());
        }
    }

    private LRUCache<String, CacheFile> get(String bucket) {
        synchronized (cache) {
            if (!cache.containsKey(bucket)) {
                cache.put(bucket,
                        new LRUCache<String, CacheFile>(CACHE_SIZE)
                                .withEvictionCallback(this));
            }
            return cache.get(bucket);
        }
    }

    @Override
    public void evicted(String key, CacheFile value)  {
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
