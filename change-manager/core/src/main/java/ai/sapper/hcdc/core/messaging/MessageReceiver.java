package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.core.connections.MessageConnection;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class MessageReceiver<I, M> implements Closeable {
    private MessageConnection connection;
    private int batchSize = 32;
    private ZookeeperConnection zkConnection;
    private String zkStatePath;
    private boolean saveState = false;

    public MessageReceiver<I, M> withConnection(@NonNull MessageConnection connection) {
        Preconditions.checkArgument(connection.isConnected());
        Preconditions.checkArgument(connection.canReceive());

        this.connection = connection;
        return this;
    }

    public MessageReceiver<I, M> withBatchSize(int batchSize) {
        if (batchSize > 0) {
            this.batchSize = batchSize;
        }
        return this;
    }

    public MessageReceiver<I, M> withZookeeperConnection(ZookeeperConnection connection) {
        zkConnection = connection;
        return this;
    }

    public MessageReceiver<I, M> withZkPath(String zkPath) {
        zkStatePath = zkPath;
        return this;
    }

    public MessageReceiver<I, M> withSaveState(boolean save) {
        saveState = save;
        return this;
    }

    public abstract MessageReceiver<I, M> init() throws MessagingError;

    public abstract MessageObject<I, M> receive() throws MessagingError;

    public abstract MessageObject<I, M> receive(long timeout) throws MessagingError;

    public abstract List<MessageObject<I, M>> nextBatch() throws MessagingError;

    public abstract List<MessageObject<I, M>> nextBatch(long timeout) throws MessagingError;

    public abstract void ack(@NonNull I messageId) throws MessagingError;

    public abstract void ack(@NonNull List<I> messageIds) throws MessagingError;
}
