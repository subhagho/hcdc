package ai.sapper.hcdc.common.model.services;

import ai.sapper.hcdc.common.model.SchemaEntity;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class SnapshotDoneRequest {
    private SchemaEntity entity;
    private long transactionId;
    private String hdfsPath;
}
