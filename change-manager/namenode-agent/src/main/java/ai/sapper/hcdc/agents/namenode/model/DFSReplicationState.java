package ai.sapper.hcdc.agents.namenode.model;

import ai.sapper.hcdc.core.model.Domain;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DFSReplicationState {
    private long inode;
    private String hdfsPath;
    private Domain entity;
    private String zkPath;
    private boolean enabled = false;
    private long snapshotTxId = -1;
    private long lastReplicatedTx = -1;
    private long snapshotTime;
    private boolean snapshotReady = false;
    private long lastReplicationTime;
    private long updateTime = 0;
}
