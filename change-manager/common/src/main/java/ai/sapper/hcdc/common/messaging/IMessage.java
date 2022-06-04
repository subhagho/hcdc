package ai.sapper.hcdc.common.messaging;

public interface IMessage<K, V> {
    K key();

    V value();
}
