package ai.sapper.hcdc.common.utils;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DefaultLogger {
    public static Logger LOG = LoggerFactory.getLogger("ai.sapper.hcdc");

    public static String error(Throwable err, String format, Object... args) {
        String mesg = String.format(format, args);
        return String.format("%s : ERROR : %s\n", mesg, err.getLocalizedMessage());
    }

    public static String stacktrace(@NonNull Throwable error) {
        StringBuilder buff = new StringBuilder(String.format("ERROR: %s", error.getLocalizedMessage()));
        buff.append("********************************BEGIN TRACE********************************\n");
        for (StackTraceElement se : error.getStackTrace()) {
            buff.append(String.format("%s[%d] : %s.%s()\n", se.getFileName(), se.getLineNumber(), se.getClassName(), se.getMethodName()));
        }
        buff.append("********************************END   TRACE********************************\n");
        return buff.toString();
    }

    public static void stacktrace(@NonNull Logger logger, @NonNull Throwable error) {
        if (logger.isDebugEnabled()) {
            logger.debug(stacktrace(error));
        }
    }
}
