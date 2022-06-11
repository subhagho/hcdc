package ai.sapper.hcdc.agents.namenode.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NameNodeTxState {
    private String namespace;
    private long updatedTime;
    private long lastTxId;
    private String currentEditsLogFile;
    private String currentFSImageFile;
}
