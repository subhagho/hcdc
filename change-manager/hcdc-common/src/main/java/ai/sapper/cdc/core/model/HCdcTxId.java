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
import ai.sapper.cdc.entity.model.TransactionId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(
        use = JsonTypeInfo.Id.CLASS,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@class"
)
public class HCdcTxId extends TransactionId {
    private long id = -1L;
    private long recordId = 0L;

    public HCdcTxId() {
    }

    public HCdcTxId(long id) {
        this.id = id;
    }

    public HCdcTxId(long id, long recordId) {
        this.id = id;
        this.recordId = recordId;
    }

    public HCdcTxId(@NonNull HCdcTxId source) {
        super(source);
        this.id = source.id;
        this.recordId = source.recordId;
    }

    public int compare(@NonNull TransactionId next) {
        long ret = -1;
        if (next instanceof HCdcTxId) {
            ret = this.id - ((HCdcTxId) next).id;
            if (ret == 0L) {
                if (isSnapshot()) {
                    ret = this.getSequence() - next.getSequence();
                } else {
                    ret = this.recordId - ((HCdcTxId) next).recordId;
                }
            }
        }
        return (int) ret;
    }

    @Override
    public String asString() {
        return String.format("%d-%d-%d-%s",
                this.id, this.recordId, this.getSequence(), String.valueOf(isSnapshot()));
    }

    @Override
    public Offset fromString(@NonNull String id) throws Exception {
        String[] parts = id.split("-");
        if (parts.length == 4) {
            this.id = Long.parseLong(parts[0]);
            this.recordId = Long.parseLong(parts[1]);
            this.setSequence(Long.parseLong(parts[2]));
            this.setSnapshot(Boolean.parseBoolean(parts[3]));
            return this;
        } else {
            throw new Exception(String.format("Invalid Transaction id. [id=%s]", id));
        }
    }

    @Override
    public int compareTo(@NonNull Offset offset) {
        Preconditions.checkArgument(offset instanceof HCdcTxId);
        return this.compare((TransactionId) offset);
    }
}
