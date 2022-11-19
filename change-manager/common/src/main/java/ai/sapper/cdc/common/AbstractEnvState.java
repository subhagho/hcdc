package ai.sapper.cdc.common;

import ai.sapper.cdc.common.AbstractState;
import lombok.NonNull;

public abstract class AbstractEnvState<T extends Enum<?>> extends AbstractState<T> {

    public AbstractEnvState(@NonNull T errorState) {
        super(errorState);
    }

    public abstract boolean isAvailable();
}
