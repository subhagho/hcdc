package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.model.LongTxState;
import lombok.NonNull;

public class UtilsEnv extends BaseEnv<LongTxState> {
    public UtilsEnv(@NonNull String name) {
        super(name);
    }
}
