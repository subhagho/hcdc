package ai.sapper.hcdc.common.model.services;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class SnapshotDoneRequest {
    private long transactionId;
    private String hdfsPath;
}
