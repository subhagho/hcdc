package ai.sapper.cdc.core.connections;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Getter
@Accessors(fluent = true)
public class KafkaConsumerConnection<K, V> extends KafkaConnection {
    private static final String CONFIG_MAX_POLL_RECORDS = "max.poll.records";
    private KafkaConsumer<K, V> consumer;
    private int batchSize = 32; // Default BatchSize is 32

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
                Properties props = kafkaConfig().properties();
                if (props.containsKey(CONFIG_MAX_POLL_RECORDS)) {
                    String s = props.getProperty(CONFIG_MAX_POLL_RECORDS);
                    batchSize = Integer.parseInt(s);
                } else {
                    props.setProperty(CONFIG_MAX_POLL_RECORDS, String.valueOf(batchSize));
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
                consumer = new KafkaConsumer<K, V>(kafkaConfig().properties());
                List<TopicPartition> parts = new ArrayList<>(kafkaConfig().partitions().size());
                for (int part : kafkaConfig().partitions()) {
                    TopicPartition tp = new TopicPartition(kafkaConfig().topic(), part);
                    parts.add(tp);
                }
                consumer.assign(parts);
                state.state(EConnectionState.Connected);
            } catch (Throwable t) {
                state.error(t);
                throw new ConnectionError("Error opening HDFS connection.", t);
            }
        }
        return this;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        synchronized (state) {
            if (state.isConnected()) {
                state.state(EConnectionState.Closed);
            }
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        }
    }

    /**
     * @return
     */
    @Override
    public boolean canSend() {
        return false;
    }

    /**
     * @return
     */
    @Override
    public boolean canReceive() {
        return true;
    }
}
