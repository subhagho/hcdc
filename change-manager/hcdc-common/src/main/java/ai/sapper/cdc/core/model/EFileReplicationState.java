package ai.sapper.cdc.core.model;

public enum EFileReplicationState {
    Unknown, New, SnapshotReady, InProgress, Finalized, Error, Stopped
}
