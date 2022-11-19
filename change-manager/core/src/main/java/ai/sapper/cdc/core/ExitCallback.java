package ai.sapper.cdc.core;

import lombok.NonNull;

public interface ExitCallback<T extends Enum<?>> {
    void call(@NonNull AbstractEnvState<T> state);
}
