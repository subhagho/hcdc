package ai.sapper.cdc.core.messaging;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.UUID;

@Getter
@Setter
@Accessors(fluent = true)
public class MessageObject<K, V> {
    public enum MessageMode {
        New, ReSend, Snapshot, Backlog, Error, Retry, Forked, Recursive, Schema
    }

    public static final String HEADER_CORRELATION_ID = "CDC_CORRELATION_ID";
    public static final String HEADER_MESSAGE_ID = "CDC_MESSAGE_ID";
    public static final String HEADER_MESSAGE_MODE = "CDC_MESSAGE_MODE";

    private String queue;
    private String id;
    private String correlationId;
    private MessageMode mode;
    private K key;
    private V value;

    public MessageObject() {
        this.id = UUID.randomUUID().toString();
    }

    public MessageObject(@NonNull String id) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
        this.id = id;
    }

    public MessageObject(@NonNull MessageObject<K, V> source) {
        this.id = source.id;
    }
}
