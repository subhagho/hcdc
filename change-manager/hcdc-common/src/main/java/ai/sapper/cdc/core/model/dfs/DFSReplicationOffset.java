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

import ai.sapper.cdc.core.model.SnapshotOffset;
import ai.sapper.cdc.core.state.Offset;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DFSReplicationOffset extends SnapshotOffset {
    private long lastReplicatedTxId = 0;

    @Override
    public String asString() {
        return String.format("%s::%d", super.asString(), lastReplicatedTxId);
    }

    @Override
    public Offset fromString(@NonNull String s) throws Exception {
        super.fromString(s);
        String[] parts = s.split("::");
        if (parts.length < 3) {
            throw new Exception(String.format("Invalid offset string. [value=%s]", s));
        }
        lastReplicatedTxId = Long.parseLong(parts[2]);
        return this;
    }

    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof DFSReplicationOffset);
        long ret = 0;
        if (lastReplicatedTxId >= 0 || ((DFSReplicationOffset) offset).lastReplicatedTxId >= 0) {
            ret = lastReplicatedTxId - ((DFSReplicationOffset) offset).lastReplicatedTxId;
        } else {
            return super.compareTo(offset);
        }
        return (int) ret;
    }
}
