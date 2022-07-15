package ai.sapper.hcdc.core.messaging;

import lombok.NonNull;

public interface KafkaPartitioner<K> {
    int partition(@NonNull K key);
}
