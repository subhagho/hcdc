package ai.sapper.cdc.core.connections.impl;

import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.KafkaProducerConnection;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class BasicKafkaProducer extends KafkaProducerConnection<String, byte[]> {
    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        super.init(xmlConfig);
        settings().getProperties()
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings().getProperties()
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return this;
    }
}
