package ai.sapper.cdc.common.cache;

import ai.sapper.cdc.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class LRUCacheTest {

    @Test
    void put() {
        try {
            LRUCache<String, String> cache = new LRUCache<>(20);
            List<String> keys = new ArrayList<>(20);
            for (int ii = 0; ii < 20; ii++) {
                String key = String.format("KEY->%d", ii);
                cache.put(key, UUID.randomUUID().toString());
                keys.add(key);
            }

            for (String key : keys) {
                Optional<String> v = cache.get(key);
                assertTrue(v.isPresent());
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    @Test
    void remove() {
        try {
            LRUCache<String, String> cache = new LRUCache<>(20);
            List<String> keys = new ArrayList<>(20);
            for (int ii = 0; ii < 20; ii++) {
                String key = String.format("KEY->%d", ii);
                cache.put(key, UUID.randomUUID().toString());
                keys.add(key);
            }

            for (int ii = 0; ii < 20; ii++) {
                if (ii % 2 == 0) continue;
                cache.remove(keys.get(ii));
            }

            for (int ii = 0; ii < 20; ii++) {
                if (ii % 2 == 0) {
                    Optional<String> v = cache.get(keys.get(ii));
                    assertTrue(v.isPresent());
                } else {
                    Optional<String> v = cache.get(keys.get(ii));
                    assertTrue(v.isEmpty());
                }
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}