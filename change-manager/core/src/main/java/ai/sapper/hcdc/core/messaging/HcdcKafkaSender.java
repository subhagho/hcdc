package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.common.messaging.ChangeDeltaMessage;
import ai.sapper.hcdc.core.connections.KafkaProducerConnection;
import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.io.IOException;

public class HcdcKafkaSender extends MessageSender<String, ChangeDeltaMessage> {
    /**
     * @param key
     * @param message
     * @throws MessagingError
     */
    @Override
    public void send(@NonNull String key, @NonNull ChangeDeltaMessage message) throws MessagingError {
        Preconditions.checkState(connection() != null);
        Preconditions.checkState(connection() instanceof HcdcKafkaConnection);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {

    }

    public static class HcdcKafkaConnection extends KafkaProducerConnection<String, byte[]> {

    }
}
