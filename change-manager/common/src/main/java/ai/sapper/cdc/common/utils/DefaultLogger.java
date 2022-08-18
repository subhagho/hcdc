package ai.sapper.cdc.common.utils;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultLogger {
    public static Logger LOG = LoggerFactory.getLogger(DefaultLogger.class);

    public static String error(Throwable err, String format, Object... args) {
        String mesg = String.format(format, args);
        return String.format("%s : ERROR : %s\n", mesg, err.getLocalizedMessage());
    }

    public static String stacktrace(@NonNull Throwable error) {
        StringBuilder buff = new StringBuilder(String.format("ERROR: %s", error.getLocalizedMessage()));
        Throwable e = error;
        while(e != null) {
            buff.append("\n********************************BEGIN TRACE********************************\n");
            buff.append(String.format("ERROR: %s\n", e.getLocalizedMessage()));
            for (StackTraceElement se : e.getStackTrace()) {
                buff.append(String.format("%s[%d] : %s.%s()\n", se.getFileName(), se.getLineNumber(), se.getClassName(), se.getMethodName()));
            }
            buff.append("********************************END   TRACE********************************\n");
            e = e.getCause();
        }
        return buff.toString();
    }

    public static void stacktrace(@NonNull Logger logger, @NonNull Throwable error) {
        if (logger.isDebugEnabled()) {
            logger.debug(stacktrace(error));
        }
    }
}
