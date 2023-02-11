package ai.sapper.cdc.common.schema;

public class StaleDataError extends Exception {
    private static final String __PREFIX = "Stale Data: %s";

    public StaleDataError(String message) {
        super(String.format(__PREFIX, message));
    }

    public StaleDataError(String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
    }

    public StaleDataError(Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
    }
}
