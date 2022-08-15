package ai.sapper.cdc.common;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractState<T extends Enum<?>> {
    private T state;
    @Setter(AccessLevel.NONE)
    private Throwable error;
    private final T errorState;

    public AbstractState(@NonNull T errorState) {
        this.errorState = errorState;
    }

    public AbstractState<T> error(@NonNull Throwable error) {
        state = errorState;
        this.error = error;
        return this;
    }

    public boolean hasError() {
        return (state == errorState);
    }

    public void clear(@NonNull T initState) {
        state = initState;
        error = null;
    }
}
