package ai.sapper.cdc.core.messaging;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class MessageObject<K, V> {
    public enum MessageMode {
        New, ReSend, Snapshot, Backlog, Error, Retry
    }

    public static final String HEADER_CORRELATION_ID = "hcdc_correlation_id";
    public static final String HEADER_MESSAGE_ID = "hcdc_message_id";
    public static final String HEADER_MESSAGE_MODE = "hcdc_message_mode";

    private String queue;
    private String id;
    private String correlationId;
    private MessageMode mode;
    private K key;
    private V value;
}
