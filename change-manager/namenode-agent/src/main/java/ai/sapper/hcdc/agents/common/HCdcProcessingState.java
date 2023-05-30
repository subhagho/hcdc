package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.messaging.kafka.KafkaOffset;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.processing.ProcessingState;
import ai.sapper.hcdc.agents.model.EHCdcProcessorState;
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
    private KafkaOffset messageOffset;

    public HCdcProcessingState() {
        super(EHCdcProcessorState.Error, EHCdcProcessorState.Unknown);
        setProcessedOffset(new HCdcTxId());
        messageOffset = new KafkaOffset();
    }

    public HCdcProcessingState(@NonNull ProcessingState<EHCdcProcessorState, HCdcTxId> state) {
        super(state);
    }
}
