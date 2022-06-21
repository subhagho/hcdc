package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.core.connections.impl.BasicKafkaProducer;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.concurrent.Future;

public class HCDCKafkaSender extends MessageSender<String, DFSChangeDelta> {
    /**
     * @param key
     * @param message
     * @throws MessagingError
     */
    @Override
    public void send(@NonNull String key, @NonNull DFSChangeDelta message) throws MessagingError {
        Preconditions.checkState(connection() != null);
        Preconditions.checkState(connection() instanceof BasicKafkaProducer);
        Preconditions.checkState(connection().isConnected());

        try {
            byte[] data = message.toByteArray();
            Future<RecordMetadata> result = ((BasicKafkaProducer) connection()).producer().send(new ProducerRecord<>(key, data));
            RecordMetadata rm = result.get();
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
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

    }
}
