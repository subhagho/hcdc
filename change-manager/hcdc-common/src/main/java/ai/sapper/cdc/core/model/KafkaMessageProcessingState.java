package ai.sapper.cdc.core.model;

import ai.sapper.cdc.core.messaging.kafka.KafkaOffset;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class KafkaMessageProcessingState extends HCdcMessageProcessingState<KafkaOffset> {
}
