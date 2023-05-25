package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.messaging.kafka.BaseKafkaConsumer;
import ai.sapper.hcdc.common.model.DFSChangeDelta;

public class HCDCKafkaReceiver extends BaseKafkaConsumer<DFSChangeDelta> {

    @Override
    protected DFSChangeDelta deserialize(byte[] bytes) throws MessagingError {
        try {
            return DFSChangeDelta.parseFrom(bytes);
        } catch (Exception ex) {
            throw new MessagingError(ex);
        }
    }
}
