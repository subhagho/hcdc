package ai.sapper.cdc.common.cache;

import lombok.NonNull;

import java.util.Optional;

public interface Cache<K, V> {
    boolean put(@NonNull K key, V value) throws Exception;

    Optional<V> get(K key);

    boolean containsKey(K key);

    int size();

    boolean isEmpty();

    void clear();

    boolean remove(K key);
}
