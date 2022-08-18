package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.utils.DefaultLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionLogger {
    private static final Logger LOG = LoggerFactory.getLogger("ai.sapper");

    public void debug(Class<?> caller, long txId, String mesg) {
        LOG.debug(String.format("[TXID=%d][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void info(Class<?> caller, long txId, String mesg) {
        LOG.info(String.format("[TXID=%d][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void warn(Class<?> caller, long txId, String mesg) {
        LOG.warn(String.format("[TXID=%d][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void error(Class<?> caller, long txId, String mesg) {
        LOG.error(String.format("[TXID=%d][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void error(Class<?> caller, long txId, Throwable t) {
        LOG.error(String.format("[TXID=%d][%s]: %s", txId, caller.getCanonicalName(), t.getLocalizedMessage()));
        if (LOG.isDebugEnabled()) {
            LOG.debug(DefaultLogger.stacktrace(t));
        }
    }

    public void error(Class<?> caller, long txId, String mesg, Throwable t) {
        LOG.error(String.format("[TXID=%d][%s]: %s", txId, caller.getCanonicalName(), mesg), t);
        if (LOG.isDebugEnabled()) {
            LOG.debug(DefaultLogger.stacktrace(t));
        }
    }

    public static final TransactionLogger LOGGER = new TransactionLogger();
}
