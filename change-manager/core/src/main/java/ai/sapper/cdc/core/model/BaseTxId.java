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
    private long recordId = 0;

    public BaseTxId() {
    }

    public BaseTxId(long txId) {
        id = txId;
    }

    public BaseTxId(long id, long recordId) {
        this.id = id;
        this.recordId = recordId;
    }

    public BaseTxId(@NonNull BaseTxId txId) {
        super(txId);
        this.id = txId.id;
        this.recordId = txId.recordId;
    }

    /**
     * @param next
     * @return
     */
    @Override
    public int compare(@NonNull TransactionId next, boolean snapshot) {
        if (next instanceof BaseTxId) {
            long ret = id - ((BaseTxId) next).id;
            if (ret == 0) {
                if (snapshot) {
                    ret = getSequence() - next.getSequence();
                } else {
                    ret = recordId - ((BaseTxId) next).recordId;
                }
            }
            return (int) ret;
        }
        return -1;
    }

    /**
     * @param id
     * @throws Exception
     */
    @Override
    public void parse(@NonNull String id) throws Exception {
        String[] parts = id.split("-");
        if (parts.length == 3) {
            this.id = Long.parseLong(parts[0]);
            this.recordId = Long.parseLong(parts[1]);
            setSequence(Long.parseLong(parts[2]));
        } else {
            throw new Exception(String.format("Invalid Transaction id. [id=%s]", id));
        }
    }

    /**
     * @return
     */
    @Override
    public String asString() {
        return String.format("%d-%d-%d", id, recordId, getSequence());
    }
}
