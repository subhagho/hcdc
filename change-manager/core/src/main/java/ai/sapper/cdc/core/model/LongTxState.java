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
public class LongTxState extends BaseTxState {

    public LongTxState() {
        setProcessedTxId(new BaseTxId());
    }
}
