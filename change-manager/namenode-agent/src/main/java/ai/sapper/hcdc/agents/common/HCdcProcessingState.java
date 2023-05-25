package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.messaging.kafka.KafkaOffset;
import ai.sapper.cdc.core.model.ProcessingState;
import ai.sapper.cdc.entity.model.BaseTxState;
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
public class HCdcProcessingState extends ProcessingState<BaseTxState> {
    private KafkaOffset messageOffset;
}
