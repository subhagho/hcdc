package ai.sapper.cdc.core.state;

import ai.sapper.cdc.core.model.KafkaMessageProcessingState;

public class KafkaProcessingStateManager extends HCdcStateManager {
    public KafkaProcessingStateManager() {
        super(KafkaMessageProcessingState.class);
    }
}
