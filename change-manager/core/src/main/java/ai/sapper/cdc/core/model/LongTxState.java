package ai.sapper.cdc.core.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class LongTxState extends ProcessingState<Long> {

    @Override
    public int compareTx(@NonNull Long target) {
        return (int) (getProcessedTxId() - target);
    }
}
