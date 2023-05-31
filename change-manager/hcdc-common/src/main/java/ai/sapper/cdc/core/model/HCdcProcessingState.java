package ai.sapper.cdc.core.model;

import ai.sapper.cdc.core.messaging.ReceiverOffset;
import ai.sapper.cdc.core.messaging.kafka.KafkaOffset;
import ai.sapper.cdc.core.processing.ProcessingState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class HCdcProcessingState extends ProcessingState<EHCdcProcessorState, HCdcTxId> {
    private ReceiverOffset messageOffset;

    public HCdcProcessingState(@NonNull ReceiverOffset messageOffset) {
        super(EHCdcProcessorState.Error, EHCdcProcessorState.Unknown);
        setProcessedOffset(new HCdcTxId());
        this.messageOffset = messageOffset;
    }

    public HCdcProcessingState(@NonNull ProcessingState<EHCdcProcessorState, HCdcTxId> state) {
        super(state);
    }
}
