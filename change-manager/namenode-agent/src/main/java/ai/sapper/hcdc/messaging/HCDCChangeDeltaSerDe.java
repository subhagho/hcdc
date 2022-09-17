package ai.sapper.hcdc.messaging;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
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
                -1,
                mode);
    }
}
