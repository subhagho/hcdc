package ai.sapper.cdc.core.messaging;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaMessageState {
    private String path;
    private String name;
    private String topic;
    private long partition = 0;
    private long offset = -1;
    private long updateTimestamp;
}
