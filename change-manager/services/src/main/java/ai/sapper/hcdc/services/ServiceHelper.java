package ai.sapper.hcdc.services;

import ai.sapper.hcdc.agents.namenode.NameNodeEnv;

public class ServiceHelper {
    public static void checkService(Object processor) throws Exception {
        if (processor == null) {
            throw new Exception("Service not initialized...");
        } else if (!NameNodeEnv.get().state().isAvailable()) {
            throw new Exception(String.format("Service environment not available. [state=%s]",
                    NameNodeEnv.get().state().state().name()));
        }
    }
}
