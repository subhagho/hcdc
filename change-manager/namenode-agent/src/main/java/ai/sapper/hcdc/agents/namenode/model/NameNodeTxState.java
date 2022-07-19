package ai.sapper.hcdc.agents.namenode.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class NameNodeTxState {
    private String namespace;
    private long updatedTime;
    private long lastTxId = 0;
    private long processedTxId = 0;
    private String currentEditsLogFile;
    private String currentFSImageFile;
}
