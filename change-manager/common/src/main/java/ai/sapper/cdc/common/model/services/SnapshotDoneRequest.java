package ai.sapper.cdc.common.model.services;

import ai.sapper.cdc.common.schema.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class SnapshotDoneRequest {
    private String domain;
    private String entity;
    private long transactionId;
    private String hdfsPath;

    public SnapshotDoneRequest() {
    }

    public SnapshotDoneRequest(@NonNull SchemaEntity entity,
                               long transactionId,
                               @NonNull String hdfsPath) {
        this.domain = entity.getDomain();
        this.entity = entity.getEntity();
        this.transactionId = transactionId;
        this.hdfsPath = hdfsPath;
    }
}
