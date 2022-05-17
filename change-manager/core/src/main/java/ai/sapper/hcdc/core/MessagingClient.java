package ai.sapper.hcdc.core;

import ai.sapper.hcdc.core.connections.Connection;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.security.PrivilegedActionException;

@Getter
@Accessors(fluent = true)
public abstract class MessagingClient<C extends Connection, M, S extends MessageSerDe<M>> {
    private C connection;
    private S transformer;

    MessagingClient(@NonNull C connection) {
        this.connection = connection;
    }

    public MessagingClient<C, M, S> withTransformer(@NonNull S transformer) {
        this.transformer = transformer;
        return this;
    }

    public int send(@NonNull M message) throws MessagingError {
        Preconditions.checkState(transformer != null);
        try {
            ByteBuffer buffer = transformer.serialize(message);
            sendMessage(buffer);

            return buffer.position();
        } catch (Throwable t) {
            throw new MessagingError("Error sending message.", t);
        }
    }

    public int publishMessage(@NonNull M message) throws MessagingError {
        Preconditions.checkState(transformer != null);
        try {
            ByteBuffer buffer = transformer.serialize(message);
            publishMessage(buffer);

            return buffer.position();
        } catch (Throwable t) {
            throw new MessagingError("Error sending message.", t);
        }
    }

    public M receive(int timeout) throws MessagingError {
        Preconditions.checkState(transformer != null);
        try {
            ByteBuffer buffer = receiveMessage(timeout);
            return transformer.deserialize(buffer);
        } catch (Throwable t) {
            throw new MessagingError("Error sending message.", t);
        }
    }

    public M publish(int timeout) throws MessagingError {
        Preconditions.checkState(transformer != null);
        try {
            ByteBuffer buffer = subscribeMessage(timeout);
            return transformer.deserialize(buffer);
        } catch (Throwable t) {
            throw new MessagingError("Error sending message.", t);
        }
    }

    public abstract void sendMessage(@NonNull ByteBuffer buffer) throws Exception;

    public abstract ByteBuffer receiveMessage(int timeout) throws Exception;

    public abstract void publishMessage(@NonNull ByteBuffer buffer) throws Exception;

    public abstract ByteBuffer subscribeMessage(int timeout) throws Exception;

    public static final class MessagingError extends Exception {
        private static final String __PREFIX = "Messaging Error";

        /**
         * Constructs a new exception with {@code null} as its detail message.
         * The cause is not initialized, and may subsequently be initialized by a
         * call to {@link #initCause}.
         */
        public MessagingError() {
            super(__PREFIX);
        }

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param message the detail message. The detail message is saved for
         *                later retrieval by the {@link #getMessage()} method.
         */
        public MessagingError(String message) {
            super(String.format("%s: %s", __PREFIX, message));
        }

        /**
         * Constructs a new exception with the specified detail message and
         * cause.  <p>Note that the detail message associated with
         * {@code cause} is <i>not</i> automatically incorporated in
         * this exception's detail message.
         *
         * @param message the detail message (which is saved for later retrieval
         *                by the {@link #getMessage()} method).
         * @param cause   the cause (which is saved for later retrieval by the
         *                {@link #getCause()} method).  (A {@code null} value is
         *                permitted, and indicates that the cause is nonexistent or
         *                unknown.)
         * @since 1.4
         */
        public MessagingError(String message, Throwable cause) {
            super(String.format("%s: %s", __PREFIX, message), cause);
        }

        /**
         * Constructs a new exception with the specified cause and a detail
         * message of {@code (cause==null ? null : cause.toString())} (which
         * typically contains the class and detail message of {@code cause}).
         * This constructor is useful for exceptions that are little more than
         * wrappers for other throwables (for example, {@link
         * PrivilegedActionException}).
         *
         * @param cause the cause (which is saved for later retrieval by the
         *              {@link #getCause()} method).  (A {@code null} value is
         *              permitted, and indicates that the cause is nonexistent or
         *              unknown.)
         * @since 1.4
         */
        public MessagingError(Throwable cause) {
            super(String.format("%s: %s", __PREFIX, cause.getLocalizedMessage()));
        }
    }
}
