package ai.sapper.cdc.core.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class LongTxState extends ProcessingState<Long> {

    public LongTxState() {
        setProcessedTxId(-1L);
    }

    @Override
    public int compareTx(Long target) {
        if (getProcessedTxId() == null) {
            if (target == null) return 0;
            else
                return Integer.MIN_VALUE;
        }
        if (target == null) return Integer.MAX_VALUE;
        return (int) (getProcessedTxId() - target);
    }
}
