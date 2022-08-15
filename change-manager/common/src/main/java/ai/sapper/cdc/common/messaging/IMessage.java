package ai.sapper.cdc.common.messaging;

public interface IMessage<K, V> {
    K key();

    V value();
}
