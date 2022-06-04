package ai.sapper.hcdc.agents.namenode;

import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class NameNodeEnv {
    private static final String NN_IGNORE_TNX = "%s.IGNORE";

    private String namespace;

    public String ignoreTnxKey() {
        return String.format(NN_IGNORE_TNX, namespace);
    }

    public static final NameNodeEnv __instance = new NameNodeEnv();

    public static NameNodeEnv get() {
        return __instance;
    }
}
