package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.core.messaging.kafka.BaseKafkaProducer;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.NonNull;

public class HCDCKafkaSender extends BaseKafkaProducer<DFSChangeDelta> {

    @Override
    protected byte[] serialize(@NonNull DFSChangeDelta dfsChangeDelta) throws MessagingError {
        return dfsChangeDelta.toByteArray();
    }
}
