package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.connections.kafka.BasicKafkaProducer;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class HCDCKafkaSender extends MessageSender<String, DFSChangeDelta> {
    private BasicKafkaProducer producer = null;
    private String topic = null;
    private KafkaPartitioner<DFSChangeDelta> partitioner;

    public HCDCKafkaSender withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public HCDCKafkaSender withPartitioner(KafkaPartitioner<DFSChangeDelta> partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    /**
     * @param message
     * @return
     * @throws MessagingError
     */
    @Override
    public MessageObject<String, DFSChangeDelta> send(@NonNull MessageObject<String, DFSChangeDelta> message) throws MessagingError {
        checkState();
        try {
            message.queue(topic);

            List<Header> headers = new ArrayList<>();
            Header h = new RecordHeader(MessageObject.HEADER_MESSAGE_ID, message.id().getBytes(StandardCharsets.UTF_8));
            headers.add(h);
            if (!Strings.isNullOrEmpty(message.correlationId())) {
                h = new RecordHeader(MessageObject.HEADER_CORRELATION_ID, message.correlationId().getBytes(StandardCharsets.UTF_8));
                headers.add(h);
            }
            if (message.mode() == null) {
                throw new MessagingError(String.format("Invalid Message Object: mode not set. [id=%s]", message.id()));
            }
            h = new RecordHeader(MessageObject.HEADER_MESSAGE_MODE, message.mode().name().getBytes(StandardCharsets.UTF_8));
            headers.add(h);

            byte[] data = message.value().toByteArray();
            Future<RecordMetadata> result = null;
            Integer partition = null;
            if (partitioner != null) {
                partition = partitioner.partition(message.value());
            }
            result = producer.producer().send(new ProducerRecord<>(topic, partition, message.key(), data, headers));
            RecordMetadata rm = result.get();

            if (auditLogger() != null) {
                auditLogger().audit(getClass(), System.currentTimeMillis(), message.value());
            }
            return message;
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    /**
     * @param messages
     * @return
     * @throws MessagingError
     */
    @Override
    public List<MessageObject<String, DFSChangeDelta>> sent(@NonNull List<MessageObject<String, DFSChangeDelta>> messages) throws MessagingError {
        checkState();
        List<MessageObject<String, DFSChangeDelta>> responses = new ArrayList<>(messages.size());
        for (MessageObject<String, DFSChangeDelta> message : messages) {
            MessageObject<String, DFSChangeDelta> response = send(message);
            responses.add(response);
        }
        return responses;
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

    private void checkState() {
        Preconditions.checkState(connection() != null);
        Preconditions.checkState(connection() instanceof BasicKafkaProducer);
        Preconditions.checkState(connection().isConnected());

        if (producer == null) {
            producer = ((BasicKafkaProducer) connection());
        }
        if (topic == null) {
            topic = producer.topic();
        }
    }
}
