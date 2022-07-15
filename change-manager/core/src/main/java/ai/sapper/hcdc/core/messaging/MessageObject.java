package ai.sapper.hcdc.core.messaging;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class MessageObject<K, V> {
    public static final String HEADER_CORRELATION_ID = "hcdc_correlation_id";
    public static final String HEADER_MESSAGE_ID = "hcdc_message_id";

    private String queue;
    private String id;
    private String correlationId;
    private K key;
    private V value;
}
