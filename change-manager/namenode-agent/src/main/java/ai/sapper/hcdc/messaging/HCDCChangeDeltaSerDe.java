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

package ai.sapper.hcdc.messaging;

import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.model.dfs.DFSFileReplicaState;
import ai.sapper.cdc.entity.model.AvroChangeType;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSFile;
import ai.sapper.hcdc.common.model.DFSSchemaChange;
import ai.sapper.hcdc.common.model.DFSTransaction;
import com.google.common.base.Preconditions;
import lombok.NonNull;

public class HCDCChangeDeltaSerDe extends ChangeDeltaSerDe {

    public static MessageObject<String, DFSChangeDelta> createSchemaChange(@NonNull DFSTransaction tnx,
                                                                           String current,
                                                                           @NonNull String updated,
                                                                           @NonNull AvroChangeType.EChangeType op,
                                                                           @NonNull DFSFileReplicaState rState,
                                                                           @NonNull MessageObject.MessageMode mode) throws Exception {
        Preconditions.checkArgument(op == AvroChangeType.EChangeType.EntityCreate ||
                op == AvroChangeType.EChangeType.EntityUpdate ||
                op == AvroChangeType.EChangeType.EntityDelete);

        DFSFile file = rState.getFileInfo().proto();
        DFSSchemaChange.Builder builder = DFSSchemaChange.newBuilder()
                .setTransaction(tnx)
                .setFile(file)
                .setUpdatedSchemaPath(updated)
                .setOp(op.opCode());
        if (current != null) {
            builder.setCurrentSchemaPath(current);
        }
        return create(builder.build(),
                DFSSchemaChange.class,
                rState.getEntity(),
                mode);
    }
}
