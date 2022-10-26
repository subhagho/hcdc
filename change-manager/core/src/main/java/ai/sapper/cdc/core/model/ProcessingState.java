package ai.sapper.cdc.core.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public abstract class ProcessingState<T> {
    private ModuleInstance instance;
    private String namespace;
    private String currentMessageId;
    private T processedTxId;
    private long updatedTime;

    public ProcessingState() {
    }

    public ProcessingState(@NonNull ProcessingState<T> state) {
        this.instance = state.instance;
        this.namespace = state.namespace;
        this.currentMessageId = state.currentMessageId;
        this.processedTxId = state.processedTxId;
        this.updatedTime = state.updatedTime;
    }

    public abstract int compareTx(T target);
}
