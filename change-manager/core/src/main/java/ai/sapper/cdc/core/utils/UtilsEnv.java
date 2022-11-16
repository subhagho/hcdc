package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.model.LongTxState;
import ai.sapper.cdc.core.schema.SchemaManager;
import lombok.NonNull;
import org.apache.commons.lang3.NotImplementedException;

public class UtilsEnv extends BaseEnv<LongTxState> {
    public UtilsEnv(@NonNull String name) {
        super(name);
    }

    @Override
    public <S extends SchemaManager> S schemaManager(@NonNull Class<? extends SchemaManager> type) throws Exception {
        throw new NotImplementedException("Should not be called...");
    }
}
