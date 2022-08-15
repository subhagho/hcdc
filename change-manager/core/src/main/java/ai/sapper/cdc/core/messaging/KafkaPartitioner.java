package ai.sapper.cdc.core.messaging;

import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface KafkaPartitioner<K> {
    void init(HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException;

    int partition(@NonNull K key);
}
