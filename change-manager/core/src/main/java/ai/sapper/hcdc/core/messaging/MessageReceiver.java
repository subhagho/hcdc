package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.core.connections.MessageConnection;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class MessageReceiver<M> implements Closeable {
    private MessageConnection connection;
    private int batchSize = 1;

    public MessageReceiver<M> withConnection(@NonNull MessageConnection connection) {
        Preconditions.checkArgument(connection.isConnected());
        Preconditions.checkArgument(connection.canReceive());

        this.connection = connection;
        return this;
    }

    public MessageReceiver<M> withBatchSize(int batchSize) {
        Preconditions.checkArgument(batchSize > 0);
        this.batchSize = batchSize;

        return this;
    }

    public abstract M receive() throws MessagingError;

    public abstract M receive(long timeout) throws MessagingError;

    public abstract List<M> nextBatch() throws MessagingError;

    public abstract List<M> nextBatch(long timeout) throws MessagingError;
}
