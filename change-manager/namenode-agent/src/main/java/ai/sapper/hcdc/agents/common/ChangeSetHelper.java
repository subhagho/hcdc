package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import ai.sapper.cdc.core.model.BlockTransactionDelta;
import ai.sapper.hcdc.agents.model.DFSBlockReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import lombok.NonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class ChangeSetHelper {
    public static boolean createChangeSet(@NonNull CDCFileSystem fs,
                                          @NonNull DFSFileState fileState,
                                          @NonNull DFSFileReplicaState replicaState,
                                          @NonNull File dest,
                                          long startTxId,
                                          long currentTxId) throws Exception {
        boolean ret = false;
        try (FileOutputStream writer = new FileOutputStream(dest)) {
            Map<Long, BlockTransactionDelta> changeSet = fileState.compressedChangeSet(startTxId, currentTxId);
            if (changeSet != null && !changeSet.isEmpty()) {
                int bufflen = 8096;
                byte[] buffer = new byte[bufflen];
                for (DFSBlockReplicaState block : replicaState.getBlocks()) {
                    if (changeSet.containsKey(block.getBlockId())) {
                        PathInfo path = fs.get(block.getStoragePath());
                        try (Reader reader = fs.reader(path)) {
                            BlockTransactionDelta delta = changeSet.get(block.getBlockId());
                            reader.seek((int) delta.getStartOffset());
                            if (delta.getStartOffset() == delta.getEndOffset()) {
                                throw new IOException(String.format("No data to read. [path=%s]", path.toString()));
                            }
                            long read = (delta.getEndOffset() - delta.getStartOffset() + 1);
                            while (read > 0) {
                                int p = (read > bufflen ? bufflen : (int) read);
                                int r = reader.read(buffer, 0, p);
                                if (r < p) throw new IOException(
                                        String.format("Error reading data: size miss-match. [path=%s]",
                                                path.toString()));
                                writer.write(buffer, 0, r);
                                read -= r;
                            }
                            ret = true;
                        }
                    }
                }
            }
        }
        return ret;
    }
}
