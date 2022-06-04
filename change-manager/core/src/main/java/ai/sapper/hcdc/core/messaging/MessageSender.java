package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.core.connections.MessageConnection;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;

@Getter
@Accessors(fluent = true)
public abstract class MessageSender<K, M> implements Closeable {
    private MessageConnection connection;

    public MessageSender<K, M> withConnection(@NonNull MessageConnection connection) {
        Preconditions.checkArgument(connection.isConnected());
        Preconditions.checkArgument(connection.canSend());

        this.connection = connection;

        return this;
    }

    public abstract void send(@NonNull K key, @NonNull M message) throws MessagingError;
}
