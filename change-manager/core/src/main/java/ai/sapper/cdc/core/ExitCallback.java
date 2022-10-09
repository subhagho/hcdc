package ai.sapper.cdc.core;

import lombok.NonNull;

public interface ExitCallback<T> {
    void call(@NonNull T state);
}
