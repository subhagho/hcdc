package ai.sapper.cdc.core.connections.kafka;

import ai.sapper.cdc.core.connections.*;
import ai.sapper.cdc.core.connections.kafka.KafkaConsumerConnection;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
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
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull ConnectionManager connectionManager) throws ConnectionError {
        super.init(xmlConfig, connectionManager);
        settings().getProperties()
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        settings().getProperties()
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        return this;
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull ConnectionManager connectionManager) throws ConnectionError {
        super.init(name, connection, path, connectionManager);
        settings().getProperties()
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        settings().getProperties()
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull ConnectionManager connectionManager) throws ConnectionError {
        super.setup(settings, connectionManager);
        settings().getProperties()
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        settings().getProperties()
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        return this;
    }
}
