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

import ai.sapper.cdc.core.model.EFileReplicationState;
import ai.sapper.cdc.core.model.EFileState;
import ai.sapper.cdc.core.state.OffsetState;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DFSFileReplicaState extends OffsetState<EFileReplicationState, DFSReplicationOffset> {
    private String hdfsPath;
    @JsonIgnore
    private DFSFileInfo fileInfo;
    @JsonIgnore
    private SchemaEntity entity;
    private String schemaDomain;
    private String schemaEntity;
    private String zkPath;
    private boolean snapshotReady = false;
    private long snapshotTime;
    private long lastReplicationTime;
    private long recordCount = 0;

    private EFileState fileState = EFileState.Unknown;
    private Map<String, String> storagePath;
    private Map<Long, DFSReplicationDelta> replicationDeltas;

    private List<DFSBlockReplicaState> blocks = new ArrayList<>();

    public DFSFileReplicaState() {
        super(EFileReplicationState.Error, EFileReplicationState.Unknown);
    }

    public void addDelta(@NonNull DFSReplicationDelta delta) {
        if (replicationDeltas == null) {
            replicationDeltas = new HashMap<>();
        }
        replicationDeltas.put(delta.getTransactionId(), delta);
    }

    public DFSReplicationDelta getDelta(long txId) {
        if (replicationDeltas != null) {
            return replicationDeltas.get(txId);
        }
        return null;
    }

    public DFSReplicationDelta removeDelta(long txId) {
        if (replicationDeltas != null && replicationDeltas.containsKey(txId)) {
            return replicationDeltas.remove(txId);
        }
        return null;
    }

    public void add(@NonNull DFSBlockReplicaState block) throws Exception {
        DFSBlockReplicaState b = get(block.getBlockId());
        if (b != null) {
            throw new Exception(String.format("Block already exists. [id=%d]", block.getBlockId()));
        }
        blocks.add(block);
    }

    public DFSBlockReplicaState get(long blockId) {
        for (DFSBlockReplicaState b : blocks) {
            if (b.getBlockId() == blockId) {
                return b;
            }
        }
        return null;
    }

    public DFSFileReplicaState copyBlocks(@NonNull DFSFileState fileState) throws Exception {
        if (fileState.hasBlocks()) {
            for (DFSBlockState bs : fileState.getBlocks()) {
                copyBlock(bs);
            }
        }
        return this;
    }

    public DFSFileReplicaState copyBlock(@NonNull DFSBlockState bs) throws Exception {
        DFSBlockReplicaState b = new DFSBlockReplicaState();
        b.setState(EFileState.New);
        b.setBlockId(bs.getBlockId());
        b.setPrevBlockId(bs.getPrevBlockId());
        b.setStartOffset(0);
        b.setDataSize(bs.getDataSize());
        b.setUpdateTime(System.currentTimeMillis());
        add(b);

        return this;
    }

    public boolean hasBlocks() {
        return !blocks.isEmpty();
    }

    public boolean canUpdate() {
        return (fileState == EFileState.New || fileState == EFileState.Updating);
    }

    @JsonIgnore
    public boolean isEnabled() {
        return (fileState != EFileState.Unknown && fileState != EFileState.Deleted && fileState != EFileState.Error);
    }

    public void clear() {
        // blocks.clear();
    }
}
