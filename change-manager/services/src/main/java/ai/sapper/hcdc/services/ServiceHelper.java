package ai.sapper.hcdc.services;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import lombok.NonNull;

public class ServiceHelper {
    public static void checkService(@NonNull String name, Object processor) throws Exception {
        if (processor == null) {
            throw new Exception("Service not initialized...");
        } else if (!NameNodeEnv.get(name).state().isAvailable()) {
            throw new Exception(String.format("Service environment not available. [state=%s]",
                    NameNodeEnv.get(name).state().state().name()));
        }
    }
}
