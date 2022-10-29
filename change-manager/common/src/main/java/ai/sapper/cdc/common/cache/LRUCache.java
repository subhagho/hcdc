package ai.sapper.cdc.common.cache;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUCache<K, V> implements Cache<K, V> {
    private int size;
    private final Map<K, LinkedListNode<CacheElement<K, V>>> nodeMap;
    private final DoublyLinkedList<CacheElement<K, V>> elements;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<EvictionCallback<K, V>> evictionCallbacks = new ArrayList<>();

    public LRUCache(int size) {
        this.size = size;
        this.nodeMap = new ConcurrentHashMap<>(size);
        this.elements = new DoublyLinkedList<>();
    }

    public LRUCache<K, V> withEvictionCallback(@NonNull EvictionCallback<K, V> callback) {
        evictionCallbacks.add(callback);
        return this;
    }

    @Override
    public boolean put(@NonNull K key, V value) {
        this.lock.writeLock().lock();
        try {
            CacheElement<K, V> item = new CacheElement<K, V>(key, value);
            LinkedListNode<CacheElement<K, V>> newNode;
            if (this.nodeMap.containsKey(key)) {
                LinkedListNode<CacheElement<K, V>> node = this.nodeMap.get(key);
                newNode = elements.updateAndMoveToFront(node, item);
            } else {
                if (this.size() >= this.size) {
                    this.evictElement();
                }
                newNode = this.elements.add(item);
            }
            if (newNode.isEmpty()) {
                return false;
            }
            this.nodeMap.put(key, newNode);
            return true;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<V> get(K key) {
        this.lock.readLock().lock();
        try {
            LinkedListNode<CacheElement<K, V>> linkedListNode = this.nodeMap.get(key);
            if (linkedListNode != null && !linkedListNode.isEmpty()) {
                nodeMap.put(key, this.elements.moveToFront(linkedListNode));
                return Optional.of(linkedListNode.getElement().getValue());
            }
            return Optional.empty();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * @param key
     * @return
     */
    @Override
    public boolean containsKey(K key) {
        return nodeMap.containsKey(key);
    }

    @Override
    public int size() {
        this.lock.readLock().lock();
        try {
            return elements.size();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void clear() {
        this.lock.writeLock().lock();
        try {
            nodeMap.clear();
            elements.clear();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public boolean remove(K key) {
        this.lock.writeLock().lock();
        try {
            if (this.nodeMap.containsKey(key)) {
                LinkedListNode<CacheElement<K, V>> node = nodeMap.remove(key);
                node = this.elements.remove(node.getElement());
                if (node != null) {
                    if (!evictionCallbacks.isEmpty()) {
                        for (EvictionCallback<K, V> callback : evictionCallbacks) {
                            callback.evicted(node.getElement().getKey(), node.getElement().getValue());
                        }
                    }
                }
                return true;
            }
        } finally {
            this.lock.writeLock().unlock();
        }
        return false;
    }


    private boolean evictElement() {
        this.lock.writeLock().lock();
        try {
            LinkedListNode<CacheElement<K, V>> node = elements.removeTail();
            if (node.isEmpty()) {
                return false;
            }
            node = nodeMap.remove(node.getElement().getKey());
            if (node != null) {
                if (!evictionCallbacks.isEmpty()) {
                    for (EvictionCallback<K, V> callback : evictionCallbacks) {
                        callback.evicted(node.getElement().getKey(), node.getElement().getValue());
                    }
                }
            }
            return true;
        } finally {
            this.lock.writeLock().unlock();
        }
    }
}