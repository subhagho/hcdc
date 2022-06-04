package ai.sapper.hcdc.common.messaging;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ChangeDeltaData {
    private String namespace;
    private String txId;
    private String entity;
    private String type;
    private long timestamp;
    private byte[] data;
}
