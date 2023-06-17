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

package ai.sapper.cdc.core.model.dfs;

import ai.sapper.cdc.core.model.EFileState;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.entity.model.BlockTransactionDelta;
import ai.sapper.hcdc.common.model.DFSFile;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class DFSFileState {
    private DFSFileInfo fileInfo;
    private String zkPath;
    private long createdTime;
    private long updatedTime;
    private long numBlocks;
    private long blockSize;
    private long dataSize;
    private long lastTnxId;
    private long timestamp;

    private EFileState state = EFileState.Unknown;

    private List<DFSBlockState> blocks;

    public DFSFileState add(@NonNull DFSBlockState block) {
        if (blocks == null)
            blocks = new ArrayList<>();
        blocks.add(block);
        numBlocks++;

        return this;
    }

    public DFSBlockState get(long blockId) {
        if (blocks != null && !blocks.isEmpty()) {
            for (DFSBlockState bs : blocks) {
                if (bs.getBlockId() == blockId) {
                    return bs;
                }
            }
        }
        return null;
    }

    public List<DFSBlockState> sortedBlocks() {
        if (blocks != null && !blocks.isEmpty()) {
            blocks.sort(new DFSBlockComparator());
        }
        return blocks;
    }

    public Map<Long, BlockTransactionDelta> compressedChangeSet(long startTxnId, long endTxnId) {
        if (blocks != null && !blocks.isEmpty()) {
            Map<Long, BlockTransactionDelta> change = new HashMap<>();
            for (DFSBlockState block : sortedBlocks()) {
                BlockTransactionDelta c = block.compressedChangeSet(startTxnId, endTxnId);
                if (c != null) {
                    change.put(block.getBlockId(), c);
                }
            }
            if (!change.isEmpty()) return change;
        }
        return null;
    }

    public DFSBlockState findFirstBlock() {
        if (blocks != null && !blocks.isEmpty()) {
            return blocks.get(0);
        }
        return null;
    }

    public DFSBlockState findLastBlock() {
        if (blocks != null && !blocks.isEmpty()) {
            return blocks.get(blocks.size() - 1);
        }
        return null;
    }

    public void reset() {
        blocks.clear();
        numBlocks = 0;
        fileInfo.setSchemaURI(null);
        fileInfo.setFileType(EFileType.UNKNOWN);
        dataSize = 0;
        blockSize = 0;
    }

    public boolean checkDeleted() {
        return (state == EFileState.Deleted);
    }

    public boolean hasError() {
        return (state == EFileState.Error);
    }

    public boolean canProcess() {
        return (!hasError() && !checkDeleted());
    }

    public boolean hasBlocks() {
        return (blocks != null && !blocks.isEmpty());
    }

    public DFSFileState withEntity(@NonNull DFSFile file) {
        fileInfo = new DFSFileInfo().parse(file);
        return this;
    }
}
