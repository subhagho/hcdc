package ai.sapper.hcdc.common.model.services;

import ai.sapper.hcdc.common.model.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class SnapshotDoneRequest {
    private SchemaEntity entity;
    private long transactionId;
    private String hdfsPath;

    public SnapshotDoneRequest() {
    }

    public SnapshotDoneRequest(@NonNull SchemaEntity entity,
                               long transactionId,
                               @NonNull String hdfsPath) {
        this.entity = entity;
        this.transactionId = transactionId;
        this.hdfsPath = hdfsPath;
    }
}
