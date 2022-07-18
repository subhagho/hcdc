package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.common.model.DFSError;
import lombok.Getter;

import java.security.PrivilegedActionException;

@Getter
public class InvalidTransactionError extends Exception {
    private static final String __PREFIX = "Invalid DFS Transaction: %s";
    private final String hdfsPath;
    private final DFSError.ErrorCode errorCode;

    /**
     * Constructs a new exception with the specified detail message.  The
     * cause is not initialized, and may subsequently be initialized by
     * a call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     */
    public InvalidTransactionError(DFSError.ErrorCode errorCode, String hdfsPath, String message) {
        super( String.format(__PREFIX, message));
        this.hdfsPath = hdfsPath;
        this.errorCode = errorCode;
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
    public InvalidTransactionError(DFSError.ErrorCode errorCode, String hdfsPath, String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
        this.hdfsPath = hdfsPath;
        this.errorCode = errorCode;
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
    public InvalidTransactionError(DFSError.ErrorCode errorCode, String hdfsPath, Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
        this.hdfsPath = hdfsPath;
        this.errorCode = errorCode;
    }
}
