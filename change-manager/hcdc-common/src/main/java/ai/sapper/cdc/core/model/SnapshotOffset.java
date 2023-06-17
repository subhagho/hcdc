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

package ai.sapper.cdc.core.model;

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
public class SnapshotOffset extends Offset {
    private long snapshotTxId = -1;
    private long snapshotSeq = -1;
    private long updatedTimestamp = -1;

    @Override
    public String asString() {
        return String.format("%d::%d", snapshotTxId, snapshotSeq);
    }

    @Override
    public Offset fromString(@NonNull String s) throws Exception {
        String[] parts = s.split("::");
        if (parts.length < 2) {
            throw new Exception(String.format("Invalid offset string. [value=%s]", s));
        }
        snapshotTxId = Long.parseLong(parts[0]);
        snapshotSeq = Long.parseLong(parts[1]);
        return this;
    }

    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof SnapshotOffset);
        long ret = snapshotTxId - ((SnapshotOffset) offset).snapshotTxId;
        if (ret == 0) {
            ret = snapshotSeq - ((SnapshotOffset) offset).snapshotSeq;
        }
        return (int) ret;
    }
}
