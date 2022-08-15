package ai.sapper.cdc.common.model.services;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class BasicResponse<T> {
    private EResponseState state;
    private Throwable error;
    private T response;

    public BasicResponse() {}

    public BasicResponse(@NonNull EResponseState state, T response) {
        this.state = state;
        this.response = response;
    }

    public BasicResponse<T> withError(@NonNull Throwable error) {
        this.error = error;
        return this;
    }
}
