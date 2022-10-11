package ai.sapper.cdc.common.cache;

public interface EvictionCallback<K, V> {
    void evicted(K key, V value);
}
