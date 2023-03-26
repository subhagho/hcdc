package ai.sapper.cdc.core.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS
)
public abstract class TransactionId {
    private EngineType type;
    private long sequence = 0;

    public TransactionId() {
    }

    public TransactionId(@NonNull TransactionId source) {
        this.type = source.type;
        this.sequence = source.sequence;
    }

    public abstract int compare(@NonNull TransactionId next, boolean snapshot);

    public abstract void parse(@NonNull String id) throws Exception;

    public abstract String asString();
}
