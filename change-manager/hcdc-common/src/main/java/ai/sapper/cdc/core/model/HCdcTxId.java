package ai.sapper.cdc.core.model;

import ai.sapper.cdc.core.state.Offset;
import ai.sapper.cdc.entity.model.TransactionId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class HCdcTxId extends TransactionId {
    private long id = -1L;
    private long recordId = 0L;

    public HCdcTxId() {
    }

    public HCdcTxId(long id) {
        this.id = id;
    }

    public HCdcTxId(long id, long recordId) {
        this.id = id;
        this.recordId = recordId;
    }

    public HCdcTxId(@NonNull HCdcTxId source) {
        super(source);
        this.id = source.id;
        this.recordId = source.recordId;
    }

    public int compare(@NonNull TransactionId next) {
        long ret = -1;
        if (next instanceof HCdcTxId) {
            ret = this.id - ((HCdcTxId) next).id;
            if (ret == 0L) {
                if (isSnapshot()) {
                    ret = this.getSequence() - next.getSequence();
                } else {
                    ret = this.recordId - ((HCdcTxId) next).recordId;
                }
            }
        }
        return (int) ret;
    }

    @Override
    public String asString() {
        return String.format("%d-%d-%d-%s",
                this.id, this.recordId, this.getSequence(), String.valueOf(isSnapshot()));
    }

    @Override
    public Offset fromString(@NonNull String id) throws Exception {
        String[] parts = id.split("-");
        if (parts.length == 4) {
            this.id = Long.parseLong(parts[0]);
            this.recordId = Long.parseLong(parts[1]);
            this.setSequence(Long.parseLong(parts[2]));
            this.setSnapshot(Boolean.parseBoolean(parts[3]));
            return this;
        } else {
            throw new Exception(String.format("Invalid Transaction id. [id=%s]", id));
        }
    }

    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof HCdcTxId);
        return this.compare((TransactionId) offset);
    }
}
