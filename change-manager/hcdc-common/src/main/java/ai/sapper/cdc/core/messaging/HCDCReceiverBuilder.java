package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.messaging.builders.MessageReceiverSettings;
import ai.sapper.cdc.core.messaging.kafka.builders.KafkaConsumerBuilder;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class HCDCReceiverBuilder extends KafkaConsumerBuilder<DFSChangeDelta> {
    public HCDCReceiverBuilder() {
        super(HCDCKafkaReceiver.class, MessageReceiverSettings.class);
    }

    public HCDCReceiverBuilder(@NonNull Class<? extends MessageReceiverSettings> settingsType) {
        super(HCDCKafkaReceiver.class, settingsType);
    }
}
