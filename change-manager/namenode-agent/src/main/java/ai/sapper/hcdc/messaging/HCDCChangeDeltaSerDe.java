package ai.sapper.hcdc.messaging;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSFile;
import ai.sapper.hcdc.common.model.DFSSchemaChange;
import ai.sapper.hcdc.common.model.DFSTransaction;
import ai.sapper.hcdc.common.utils.SchemaEntityHelper;
import com.google.common.base.Preconditions;
import lombok.NonNull;

public class HCDCChangeDeltaSerDe extends ChangeDeltaSerDe {

    public static MessageObject<String, DFSChangeDelta> createSchemaChange(@NonNull String namespace,
                                                                           @NonNull DFSTransaction tnx,
                                                                           @NonNull SchemaVersion current,
                                                                           @NonNull SchemaVersion updated,
                                                                           @NonNull AvroChangeType.EChangeType op,
                                                                           @NonNull DFSFileReplicaState rState,
                                                                           @NonNull MessageObject.MessageMode mode) throws Exception {
        Preconditions.checkArgument(op == AvroChangeType.EChangeType.EntityCreate ||
                op == AvroChangeType.EChangeType.EntityUpdate ||
                op == AvroChangeType.EChangeType.EntityDelete);

        DFSFile file = DFSFile.newBuilder()
                .setPath(rState.getHdfsPath())
                .setInodeId(rState.getInode())
                .build();
        DFSSchemaChange change = DFSSchemaChange.newBuilder()
                .setTransaction(tnx)
                .setFile(file)
                .setSchema(SchemaEntityHelper.proto(rState.getEntity()))
                .setCurrentSchema(current.toString())
                .setUpdatedSchema(updated.toString())
                .setOp(op.opCode())
                .build();
        return create(namespace, change,
                DFSSchemaChange.class,
                rState.getEntity(),
                mode);
    }
}
