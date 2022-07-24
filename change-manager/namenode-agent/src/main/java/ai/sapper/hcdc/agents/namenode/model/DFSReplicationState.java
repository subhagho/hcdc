package ai.sapper.hcdc.agents.namenode.model;

import ai.sapper.hcdc.common.model.SchemaEntity;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DFSReplicationState {
    private long inode;
    private String hdfsPath;
    private SchemaEntity entity;
    private String zkPath;
    private boolean enabled = false;
    private long snapshotTxId = -1;
    private long lastReplicatedTx = -1;
    private long snapshotTime;
    private boolean snapshotReady = false;
    private long lastReplicationTime;
    private long updateTime = 0;
}
