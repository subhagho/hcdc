package ai.sapper.cdc.core.messaging;

import lombok.NonNull;

public interface AckDelegate<M> {
    void ack(@NonNull M message) throws MessagingError;
}
