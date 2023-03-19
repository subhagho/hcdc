package ai.sapper.cdc.core.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class BaseTxState extends ProcessingState<BaseTxId> {
    /**
     * @param baseTxId
     * @return
     */
    @Override
    public int compareTx(BaseTxId baseTxId) {
        if (baseTxId != null) {
            return this.getProcessedTxId().compare(baseTxId, false);
        }
        return 1;
    }
}
