package ai.sapper.cdc.core.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class BaseTxId extends TransactionId {
    private long id = -1;

    public BaseTxId() {
    }

    public BaseTxId(long txId) {
        id = txId;
    }

    /**
     * @param next
     * @return
     */
    @Override
    public int compare(@NonNull TransactionId next, boolean snapshot) {
        if (next instanceof BaseTxId) {
            return (int) (id - ((BaseTxId) next).id);
        }
        return -1;
    }

    /**
     * @param id
     * @throws Exception
     */
    @Override
    public void parse(@NonNull String id) throws Exception {
        this.id = Long.parseLong(id);
    }

    /**
     * @return
     */
    @Override
    public String asString() {
        return String.valueOf(id);
    }
}
