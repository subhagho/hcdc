package ai.sapper.cdc.core;

import lombok.NonNull;

public interface ExitCallback {
    void call(@NonNull Object state);
}
