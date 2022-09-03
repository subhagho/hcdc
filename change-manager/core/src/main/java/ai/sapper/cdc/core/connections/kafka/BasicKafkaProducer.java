package ai.sapper.cdc.core.connections.kafka;

import ai.sapper.cdc.core.connections.*;
import ai.sapper.cdc.core.connections.kafka.KafkaProducerConnection;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
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
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                           @NonNull ConnectionManager connectionManager) throws ConnectionError {
        super.init(xmlConfig, connectionManager);
        settings().getProperties()
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings().getProperties()
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return this;
    }

    @Override
    public Connection init(@NonNull String name,
                           @NonNull ZookeeperConnection connection,
                           @NonNull String path,
                           @NonNull ConnectionManager connectionManager) throws ConnectionError {
        super.init(name, connection, path, connectionManager);
        settings().getProperties()
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings().getProperties()
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return this;
    }

    @Override
    public Connection setup(@NonNull ConnectionSettings settings,
                            @NonNull ConnectionManager connectionManager) throws ConnectionError {
        super.setup(settings, connectionManager);
        settings().getProperties()
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings().getProperties()
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return this;
    }
}
