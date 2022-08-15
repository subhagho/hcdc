package ai.sapper.cdc.core.messaging;

import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

public class KafkaMessage<K, V> extends MessageObject<K, V> {

    public KafkaMessage() {
    }

    public KafkaMessage(@NonNull ConsumerRecord<K, ?> record, @NonNull V value) {
        queue(record.topic());
        key(record.key());
        value(value);

        Header ih = record.headers().lastHeader(HEADER_MESSAGE_ID);
        if (ih != null && ih.value() != null) {
            String mid = new String(ih.value(), StandardCharsets.UTF_8);
            id(mid);
        } else
            id(String.format("%s:%d:%d", record.topic(), record.partition(), record.offset()));

        Header ch = record.headers().lastHeader(HEADER_CORRELATION_ID);
        if (ch != null && ch.value() != null) {
            String cid = new String(ch.value(), StandardCharsets.UTF_8);
            correlationId(cid);
        }
        Header mh = record.headers().lastHeader(HEADER_MESSAGE_MODE);
        if (mh != null && mh.value() != null) {
            String m = new String(mh.value(), StandardCharsets.UTF_8);
            mode(MessageMode.valueOf(m));
        }
    }
}
