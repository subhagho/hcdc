package ai.sapper.cdc.common.cache;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LRUCache<K, V> implements Cache<K, V> {
    private int size;
    private Map<K, LinkedListNode<CacheElement<K, V>>> nodeMap;
    private DoublyLinkedList<CacheElement<K, V>> elementLinkedList;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public LRUCache(int size) {
        this.size = size;
        this.nodeMap = new ConcurrentHashMap<>(size);
        this.elementLinkedList = new DoublyLinkedList<>();
    }

    @Override
    public boolean put(K key, V value) {
        this.lock.writeLock().lock();
        try {
            CacheElement<K, V> item = new CacheElement<K, V>(key, value);
            LinkedListNode<CacheElement<K, V>> newNode;
            if (this.nodeMap.containsKey(key)) {
                LinkedListNode<CacheElement<K, V>> node = this.nodeMap.get(key);
                newNode = elementLinkedList.updateAndMoveToFront(node, item);
            } else {
                if (this.size() >= this.size) {
                    this.evictElement();
                }
                newNode = this.elementLinkedList.add(item);
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
                nodeMap.put(key, this.elementLinkedList.moveToFront(linkedListNode));
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
            return elementLinkedList.size();
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
            elementLinkedList.clear();
        } finally {
            this.lock.writeLock().unlock();
        }
    }


    private boolean evictElement() {
        this.lock.writeLock().lock();
        try {
            LinkedListNode<CacheElement<K, V>> linkedListNode = elementLinkedList.removeTail();
            if (linkedListNode.isEmpty()) {
                return false;
            }
            nodeMap.remove(linkedListNode.getElement().getKey());
            return true;
        } finally {
            this.lock.writeLock().unlock();
        }
    }
}