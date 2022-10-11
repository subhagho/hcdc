package ai.sapper.cdc.common.cache;

import java.util.Optional;

public interface Cache<K, V> {
    boolean put(K key, V value) throws Exception;

    Optional<V> get(K key);

    boolean containsKey(K key);

    int size();

    boolean isEmpty();

    void clear();

}
