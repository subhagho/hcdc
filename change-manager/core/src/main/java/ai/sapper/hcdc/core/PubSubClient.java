package ai.sapper.hcdc.core;

import ai.sapper.hcdc.core.connections.Connection;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.security.PrivilegedActionException;

@Getter
@Accessors(fluent = true)
public abstract class PubSubClient<C extends Connection, M, S extends MessageSerDe<M>> implements Closeable {
    private C connection;
    private S transformer;
    private int receiveBatchSize = 1;
    private int sendBatchSize = 1;

    PubSubClient(@NonNull C connection) {
        this.connection = connection;
    }

    public PubSubClient<C, M, S> withTransformer(@NonNull S transformer) {
        this.transformer = transformer;
        return this;
    }

    public PubSubClient<C, M, S> withReceiveBatchSize(int batchSize) {
        Preconditions.checkArgument(batchSize > 0);
        receiveBatchSize = batchSize;

        return this;
    }

    public PubSubClient<C, M, S> withSendBatchSize(int batchSize) {
        Preconditions.checkArgument(batchSize > 0);
        sendBatchSize = batchSize;

        return this;
    }

    public int publish(@NonNull M message) throws MessagingError {
        Preconditions.checkState(transformer != null);
        try {
            ByteBuffer buffer = transformer.serialize(message);
            publishMessage(buffer);

            return buffer.position();
        } catch (Throwable t) {
            throw new MessagingError("Error sending message.", t);
        }
    }

    public M subscribe(int timeout) throws MessagingError {
        Preconditions.checkState(transformer != null);
        try {
            ByteBuffer buffer = subscribeMessage(timeout);
            return transformer.deserialize(buffer);
        } catch (Throwable t) {
            throw new MessagingError("Error sending message.", t);
        }
    }

    public abstract void open(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws MessagingError;
    public abstract void publishMessage(@NonNull ByteBuffer buffer) throws MessagingError;
    public abstract ByteBuffer subscribeMessage(int timeout) throws MessagingError;

    public abstract boolean ack(@NonNull String messageId) throws MessagingError;
}
