package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.core.connections.impl.BasicKafkaConsumer;
import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;

public class HCDCKafkaReceiver extends MessageReceiver<String, byte[]> {
    /**
     * @return
     * @throws MessagingError
     */
    @Override
    public AbstractMap.SimpleEntry<String, byte[]> receive() throws MessagingError {
        return null;
    }

    /**
     * @param timeout
     * @return
     * @throws MessagingError
     */
    @Override
    public AbstractMap.SimpleEntry<String, byte[]> receive(long timeout) throws MessagingError {
        return null;
    }

    /**
     * @return
     * @throws MessagingError
     */
    @Override
    public List<AbstractMap.SimpleEntry<String, byte[]>> nextBatch() throws MessagingError {
        return null;
    }

    /**
     * @param timeout
     * @return
     * @throws MessagingError
     */
    @Override
    public List<AbstractMap.SimpleEntry<String, byte[]>> nextBatch(long timeout) throws MessagingError {
        return null;
    }

    /**
     * @param messageIds
     * @throws MessagingError
     */
    @Override
    public void ack(@NonNull List<String> messageIds) throws MessagingError {

    }

    /**
     * @param messageId
     * @throws MessagingError
     */
    @Override
    public void ack(@NonNull String messageId) throws MessagingError {

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

    private void checkState() {
        Preconditions.checkState(connection() != null);
        Preconditions.checkState(connection() instanceof BasicKafkaConsumer);
        Preconditions.checkState(connection().isConnected());
    }
}
