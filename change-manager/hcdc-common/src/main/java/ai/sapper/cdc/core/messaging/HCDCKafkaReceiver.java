package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.connections.kafka.BasicKafkaConsumer;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
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
            this.offset = new OffsetAndMetadata(record.offset(), String.format("[Key=%s]", key));
        }
    }

    private static final long DEFAULT_RECEIVE_TIMEOUT = 30000; // 30 secs default timeout.
    private Queue<MessageObject<String, DFSChangeDelta>> cache = null;
    private final Map<String, OffsetData> offsetMap = new HashMap<>();

    private BasicKafkaConsumer consumer = null;
    private String topic;
    private KafkaStateManager stateManager = null;
    private Map<Integer, KafkaMessageState> states = null;

    public void seek(TopicPartition partition, long offset) {
        if (offset > 0) {
            consumer.consumer().seek(partition, offset);
        } else {
            consumer.consumer().seekToBeginning(Collections.singletonList(partition));
        }
    }

    /**
     * @return
     * @throws MessagingError
     */
    @Override
    public MessageReceiver<String, DFSChangeDelta> init() throws MessagingError {
        Preconditions.checkState(connection() instanceof BasicKafkaConsumer);
        consumer = (BasicKafkaConsumer) connection();
        cache = new ArrayBlockingQueue<>(consumer.batchSize());
        topic = consumer.topic();

        try {
            if (!consumer.isConnected()) {
                consumer.connect();
            }

            if (saveState()) {
                Preconditions.checkState(zkConnection() != null);
                Preconditions.checkState(!Strings.isNullOrEmpty(zkStatePath()));
                if (!zkConnection().isConnected())
                    zkConnection().connect();
                stateManager = new KafkaStateManager(consumer.name(), topic, zkConnection(), zkStatePath());
                states = new HashMap<>();
                Set<TopicPartition> partitions = consumer.consumer().assignment();
                if (partitions == null || partitions.isEmpty()) {
                    throw new MessagingError(String.format("No assigned partitions found. [name=%s][topic=%s]",
                            consumer.name(), topic));
                }
                for (TopicPartition partition : partitions) {
                    KafkaMessageState state = stateManager.getState(partition.partition());
                    Preconditions.checkNotNull(state);
                    seek(partition, state.getOffset() + 1);
                    states.put(partition.partition(), state);
                }
            }
            return this;
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }

    private KafkaMessageState updateState(int partition, long offset) throws Exception {
        if (saveState()) {
            KafkaMessageState state = states.get(partition);
            if (state == null) {
                throw new MessagingError(
                        String.format("Kafka State not registered for partition. [partition=%d][topic=%s]",
                                partition, topic));
            }
            state = stateManager.updateState(partition, offset);
            states.put(partition, state);

            return state;
        }
        return null;
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
            ConsumerRecords<String, byte[]> records = consumer.consumer().poll(Duration.ofMillis(timeout));
            if (records != null && records.count() > 0) {
                List<MessageObject<String, DFSChangeDelta>> array = new ArrayList<>(records.count());
                for (ConsumerRecord<String, byte[]> record : records) {
                    DFSChangeDelta cd = DFSChangeDelta.parseFrom(record.value());
                    KafkaMessage<String, DFSChangeDelta> response = new KafkaMessage<>(record, cd);

                    if (auditLogger() != null) {
                        auditLogger().audit(getClass(), System.currentTimeMillis(), response.value());
                    }
                    array.add(response);
                    offsetMap.put(response.id(), new OffsetData(record.key(), record));
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
                    updateState(od.partition.partition(), od.offset.offset());
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

                updateState(od.partition.partition(), od.offset.offset());
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
        Preconditions.checkState(connection().isConnected());
        Preconditions.checkState(consumer != null);
        Preconditions.checkState(cache != null);
        Preconditions.checkState(!Strings.isNullOrEmpty(topic));
    }
}
