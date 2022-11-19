package ai.sapper.cdc.core;

import ai.sapper.cdc.common.AbstractEnvState;
import lombok.NonNull;

public interface ExitCallback<T extends Enum<?>> {
    void call(@NonNull AbstractEnvState<T> state);
}
