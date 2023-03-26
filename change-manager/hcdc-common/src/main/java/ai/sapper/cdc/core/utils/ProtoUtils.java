package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.cdc.core.model.EngineType;
import ai.sapper.cdc.entity.model.*;
import ai.sapper.hcdc.common.model.DFSTransaction;
import lombok.NonNull;

public class ProtoUtils {
    public static DFSTransaction buildTx(@NonNull BaseTxId txId,
                                         @NonNull DFSTransaction.Operation op) {
        return DFSTransaction.newBuilder()
                .setId(txId.getId())
                .setSequence(txId.getSequence())
                .setRecordId(txId.getRecordId())
                .setOp(op)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    public static BaseTxId fromTx(@NonNull DFSTransaction tx) {
        BaseTxId id = new BaseTxId();
        id.setType(EngineType.HDFS);
        id.setId(tx.getId());
        id.setSequence(tx.getSequence());
        id.setRecordId(tx.getRecordId());
        return id;
    }

    public static String toString(@NonNull DFSTransaction tx) {
        return String.format("%s-%d-%d-%d",
                tx.getOp().name(),
                tx.getId(),
                tx.getSequence(),
                tx.getRecordId());
    }

    public static Transaction build(@NonNull BaseTxId txId) {
        Sequence sequence = Sequence.newBuilder()
                .setMajor(txId.getSequence())
                .setMinor((int) txId.getRecordId())
                .build();
        return Transaction.newBuilder()
                .setId(txId.asString())
                .setTimestamp(System.currentTimeMillis())
                .setClass_(txId.getClass().getCanonicalName())
                .setSequence(sequence)
                .build();
    }

    public static DbSource build(@NonNull String name,
                                 @NonNull String host,
                                 @NonNull String user,
                                 int port) {
        return DbSource.newBuilder()
                .setName(name)
                .setHostname(host)
                .setPort(port)
                .setType(DbSource.EngineType.HDFS)
                .setUsername(user)
                .build();
    }

    public static DbEntity build(@NonNull SchemaEntity entity,
                                 @NonNull String path) {
        return DbEntity.newBuilder()
                .setDatabase(entity.getDomain())
                .setTable(entity.getEntity())
                .setGroup(entity.getGroup())
                .setSourcePath(path)
                .build();
    }

    public static Version build(@NonNull SchemaVersion version) {
        return Version.newBuilder()
                .setMajor(version.getMajorVersion())
                .setMinor(version.getMinorVersion())
                .build();
    }

    public static DbChangeRecord.ChangeType changeType(@NonNull AvroChangeType.EChangeType type) {
        switch (type) {
            case RecordInsert:
                return DbChangeRecord.ChangeType.CREATE;
            case RecordUpdate:
                return DbChangeRecord.ChangeType.UPDATE;
            case RecordDelete:
                return DbChangeRecord.ChangeType.DELETE;
        }
        return null;
    }
}
