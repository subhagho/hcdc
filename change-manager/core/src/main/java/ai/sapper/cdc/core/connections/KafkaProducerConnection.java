package ai.sapper.cdc.core.connections;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.naming.ConfigurationException;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class KafkaProducerConnection<K, V> extends KafkaConnection {
    private KafkaProducer<K, V> producer;

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
                if (kafkaConfig().mode() != EKafkaClientMode.Producer) {
                    throw new ConfigurationException("Connection not initialized in Producer mode.");
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
            if (!state.isConnected()) {
                try {
                    producer = new KafkaProducer<K, V>(kafkaConfig().properties());

                    state.state(EConnectionState.Connected);
                } catch (Throwable t) {
                    state.error(t);
                    throw new ConnectionError("Error opening HDFS connection.", t);
                }
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
            if (producer != null) {
                producer.close();
                producer = null;
            }
        }
    }

    /**
     * @return
     */
    @Override
    public boolean canSend() {
        return true;
    }

    /**
     * @return
     */
    @Override
    public boolean canReceive() {
        return false;
    }
}
