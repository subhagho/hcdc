package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.messaging.builders.MessageSenderSettings;
import ai.sapper.cdc.core.messaging.kafka.builders.KafkaProducerBuilder;
import ai.sapper.cdc.core.messaging.kafka.builders.KafkaProducerSettings;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class HCDCSenderBuilder extends KafkaProducerBuilder<DFSChangeDelta> {
    public HCDCSenderBuilder() {
        super(HCDCKafkaSender.class, KafkaProducerSettings.class);
    }

    public HCDCSenderBuilder(@NonNull Class<? extends MessageSenderSettings> settingsType) {
        super(HCDCKafkaSender.class, settingsType);
    }
}
