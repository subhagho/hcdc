package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.core.connections.impl.BasicKafkaConsumer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class HCDCKafkaReceiver extends MessageReceiver<String, DFSChangeDelta> {
    private static class OffsetData {
        private final String key;
        private final TopicPartition partition;
        private final OffsetAndMetadata offset;

        public OffsetData(String key, ConsumerRecord<String, byte[]> record) {
            this.key = key;
            this.partition = new TopicPartition(record.topic(), record.partition());
            this.offset = new OffsetAndMetadata(record.offset() + 1, String.format("[Key=%s]", key));
        }
    }

    private static final long DEFAULT_RECEIVE_TIMEOUT = 30000; // 30 secs default timeout.
    private Queue<MessageObject<String, DFSChangeDelta>> cache = null;
    private Map<String, OffsetData> offsetMap = new HashMap<>();

    private BasicKafkaConsumer consumer = null;
    private String topic;

    public HCDCKafkaReceiver withTopic(@NonNull String topic) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(topic));
        this.topic = topic;

        return this;
    }

    /**
     * @return
     * @throws MessagingError
     */
    @Override
    public MessageObject<String, DFSChangeDelta> receive() throws MessagingError {
        return receive(DEFAULT_RECEIVE_TIMEOUT);
    }

    /**
     * @param timeout
     * @return
     * @throws MessagingError
     */
    @Override
    public MessageObject<String, DFSChangeDelta> receive(long timeout) throws MessagingError {
        checkState();
        if (cache.isEmpty()) {
            List<MessageObject<String, DFSChangeDelta>> batch = nextBatch(timeout);
            if (batch != null) {
                cache.addAll(batch);
            }
        }
        if (!cache.isEmpty()) {
            return cache.poll();
        }
        return null;
    }

    /**
     * @return
     * @throws MessagingError
     */
    @Override
    public List<MessageObject<String, DFSChangeDelta>> nextBatch() throws MessagingError {
        return nextBatch(DEFAULT_RECEIVE_TIMEOUT);
    }

    /**
     * @param timeout
     * @return
     * @throws MessagingError
     */
    @Override
    public List<MessageObject<String, DFSChangeDelta>> nextBatch(long timeout) throws MessagingError {
        checkState();
        try {
            ConsumerRecords<String, byte[]> records = consumer.consumer().poll(timeout);
            if (records != null && records.count() > 0) {
                List<MessageObject<String, DFSChangeDelta>> array = new ArrayList<>(records.count());
                for (ConsumerRecord<String, byte[]> record : records) {
                    DFSChangeDelta cd = DFSChangeDelta.parseFrom(record.value());
                    KafkaMessage<String, DFSChangeDelta> response = new KafkaMessage<>(record, cd);

                    array.add(response);
                    offsetMap.put(record.key(), new OffsetData(record.key(), record));
                }
                return array;
            }
            return null;
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    /**
     * @param messageIds
     * @throws MessagingError
     */
    @Override
    public void ack(@NonNull List<String> messageIds) throws MessagingError {
        checkState();
        Preconditions.checkArgument(!messageIds.isEmpty());
        try {
            Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                    new HashMap<>();
            for (String messageId : messageIds) {
                if (offsetMap.containsKey(messageId)) {
                    OffsetData od = offsetMap.get(messageId);
                    currentOffsets.put(od.partition, od.offset);
                } else {
                    throw new MessagingError(String.format("No record offset found for key. [key=%s]", messageId));
                }
            }
            consumer.consumer().commitSync(currentOffsets);
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    /**
     * @param messageId
     * @throws MessagingError
     */
    @Override
    public void ack(@NonNull String messageId) throws MessagingError {
        checkState();
        try {
            if (offsetMap.containsKey(messageId)) {
                OffsetData od = offsetMap.get(messageId);
                Map<TopicPartition, OffsetAndMetadata> currentOffsets =
                        new HashMap<>();
                currentOffsets.put(od.partition, od.offset);
                consumer.consumer().commitSync(currentOffsets);
            } else {
                throw new MessagingError(String.format("No record offset found for key. [key=%s]", messageId));
            }
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

    private synchronized void checkState() {
        Preconditions.checkState(connection() != null);
        Preconditions.checkState(connection() instanceof BasicKafkaConsumer);
        Preconditions.checkState(connection().isConnected());

        if (consumer == null) {
            consumer = (BasicKafkaConsumer) connection();
        }
        if (cache == null) {
            cache = new ArrayBlockingQueue<>(consumer.batchSize());
        }
        if (topic == null) {
            topic = consumer.topic();
        }
    }
}
