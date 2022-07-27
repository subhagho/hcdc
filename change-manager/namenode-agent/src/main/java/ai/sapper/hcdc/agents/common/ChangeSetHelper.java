package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.agents.namenode.model.DFSFileReplicaState;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.model.BlockTransactionDelta;
import ai.sapper.hcdc.core.model.DFSFileState;
import lombok.NonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class ChangeSetHelper {
    public static boolean createChangeSet(@NonNull FileSystem fs,
                                          @NonNull DFSFileState fileState,
                                          @NonNull DFSFileReplicaState replicaState,
                                          @NonNull File dest,
                                          long txId) throws Exception {
        boolean ret = false;
        try (FileOutputStream writer = new FileOutputStream(dest)) {
            Map<Long, BlockTransactionDelta> changeSet = fileState.compressedChangeSet(-1, txId);
            if (changeSet != null && !changeSet.isEmpty()) {
                for (long blockId : changeSet.keySet()) {

                }
            }
        }
        return ret;
    }
}
