package ai.sapper.hcdc.core.connections;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.naming.ConfigurationException;

@Getter
@Accessors(fluent = true)
public class KafkaConsumerConnection<K, V> extends KafkaConnection {
    private KafkaConsumer<K, V> consumer;

    /**
     * @param xmlConfig
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConnectionError {
        synchronized (state) {
            super.init(xmlConfig);
            try {
                if (kafkaConfig().mode() != EKafkaClientMode.Consumer) {
                    throw new ConfigurationException("Connection not initialized in Consumer mode.");
                }

                state.state(EConnectionState.Initialized);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    /**
     * @return
     * @throws ConnectionError
     */
    @Override
    public Connection connect() throws ConnectionError {
        synchronized (state) {
            Preconditions.checkState(connectionState() == EConnectionState.Initialized);
            try {
               consumer = new KafkaConsumer<K, V>(kafkaConfig().consumerProperties());

                state.state(EConnectionState.Connected);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }
}
