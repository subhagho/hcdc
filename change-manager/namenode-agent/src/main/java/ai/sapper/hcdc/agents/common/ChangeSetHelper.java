/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.model.dfs.DFSBlockReplicaState;
import ai.sapper.cdc.core.model.dfs.DFSFileReplicaState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.entity.model.BlockTransactionDelta;
import lombok.NonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class ChangeSetHelper {
    public static boolean createChangeSet(@NonNull FileSystem fs,
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
                        PathInfo path = fs.parsePathInfo(block.getStoragePath());
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
