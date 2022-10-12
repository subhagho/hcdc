package ai.sapper.cdc.core.model;

import lombok.Getter;
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

    public abstract int compareTx(T target);
}
