package ai.sapper.hcdc.core.messaging;

public interface IMessage<K, V> {
    K key();

    V value();
}
