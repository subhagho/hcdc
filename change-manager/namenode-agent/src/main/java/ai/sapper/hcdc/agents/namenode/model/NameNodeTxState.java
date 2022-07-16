package ai.sapper.hcdc.agents.namenode.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NameNodeTxState {
    private String namespace;
    private long updatedTime;
    private long lastTxId = 0;
    private long processedTxId = 0;
    private String currentEditsLogFile;
    private String currentFSImageFile;
}
