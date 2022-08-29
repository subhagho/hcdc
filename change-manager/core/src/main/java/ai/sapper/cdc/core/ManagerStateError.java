package ai.sapper.cdc.core;

public class ManagerStateError extends Exception {
    private static final String __PREFIX = "Manager State Error: %s";

    public ManagerStateError(String message) {
        super(String.format(__PREFIX, message));
    }

    public ManagerStateError(String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
    }

    public ManagerStateError(Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
    }
}
