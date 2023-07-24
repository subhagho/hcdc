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

package ai.sapper.cdc.core.executor;

import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.entity.executor.EntityTask;
import com.google.common.base.Preconditions;
import lombok.NonNull;

public class HCdcShardedExecutor extends BaseShardedExecutor<HCdcTxId> {
    @Override
    public int getShard(@NonNull BaseTask<HCdcTxId> baseTask, int shardCount) {
        Preconditions.checkArgument(baseTask instanceof EntityTask<HCdcTxId>);
        String key = ((EntityTask<HCdcTxId>) baseTask).entity().toString();
        int hash = key.hashCode();
        return (hash % shardCount);
    }
}
