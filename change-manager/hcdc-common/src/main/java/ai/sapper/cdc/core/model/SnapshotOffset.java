package ai.sapper.cdc.core.model;

import ai.sapper.cdc.core.state.Offset;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SnapshotOffset extends Offset {
    private long snapshotTxId = -1;
    private long snapshotSeq = -1;
    private long updatedTimestamp = -1;

    @Override
    public String asString() {
        return String.format("%d::%d", snapshotTxId, snapshotSeq);
    }

    @Override
    public Offset fromString(@NonNull String s) throws Exception {
        String[] parts = s.split("::");
        if (parts.length < 2) {
            throw new Exception(String.format("Invalid offset string. [value=%s]", s));
        }
        snapshotTxId = Long.parseLong(parts[0]);
        snapshotSeq = Long.parseLong(parts[1]);
        return this;
    }

    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof SnapshotOffset);
        long ret = snapshotTxId - ((SnapshotOffset) offset).snapshotTxId;
        if (ret == 0) {
            ret = snapshotSeq - ((SnapshotOffset) offset).snapshotSeq;
        }
        return (int) ret;
    }
}
