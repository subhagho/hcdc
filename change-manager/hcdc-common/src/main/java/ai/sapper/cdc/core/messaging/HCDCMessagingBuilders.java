package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.messaging.builders.MessageReceiverSettings;
import ai.sapper.cdc.core.messaging.kafka.builders.KafkaConsumerBuilder;
import ai.sapper.cdc.core.messaging.kafka.builders.KafkaProducerBuilder;
import ai.sapper.cdc.core.messaging.kafka.builders.KafkaProducerSettings;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

public class HCDCMessagingBuilders {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class SenderBuilder extends KafkaProducerBuilder<DFSChangeDelta> {

        public SenderBuilder(@NonNull BaseEnv<?> env) {
            super(HCDCKafkaSender.class, env, KafkaProducerSettings.class);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ReceiverBuilder extends KafkaConsumerBuilder<DFSChangeDelta> {

        public ReceiverBuilder(@NonNull BaseEnv<?> env) {
            super(HCDCKafkaReceiver.class, env, MessageReceiverSettings.class);
        }
    }
}
