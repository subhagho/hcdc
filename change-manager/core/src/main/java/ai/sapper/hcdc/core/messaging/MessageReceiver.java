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
public abstract class MessageReceiver<I, M extends MessageHandle<I, M>> implements Closeable {
    private MessageConnection connection;
    private int batchSize = 1;

    public MessageReceiver<I, M> withConnection(@NonNull MessageConnection connection) {
        Preconditions.checkArgument(connection.isConnected());
        Preconditions.checkArgument(connection.canReceive());

        this.connection = connection;
        return this;
    }

    public MessageReceiver<I, M> withBatchSize(int batchSize) {
        Preconditions.checkArgument(batchSize > 0);
        this.batchSize = batchSize;

        return this;
    }

    public abstract MessageHandle<I, M> receive() throws MessagingError;

    public abstract MessageHandle<I, M> receive(long timeout) throws MessagingError;

    public abstract List<MessageHandle<I, M>> nextBatch() throws MessagingError;

    public abstract List<MessageHandle<I, M>> nextBatch(long timeout) throws MessagingError;

    public abstract void ack(@NonNull I messageId) throws MessagingError;

    public abstract void ack(@NonNull List<I> messageIds) throws MessagingError;
}
