package ai.sapper.hcdc.core.connections.impl;

import ai.sapper.hcdc.core.connections.Connection;
import ai.sapper.hcdc.core.connections.ConnectionError;
import ai.sapper.hcdc.core.connections.KafkaConsumerConnection;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class BasicKafkaConsumer extends KafkaConsumerConnection<String, byte[]> {
    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        super.init(xmlConfig);
        kafkaConfig().consumerProperties().put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        kafkaConfig().consumerProperties().put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        return this;
    }
}
