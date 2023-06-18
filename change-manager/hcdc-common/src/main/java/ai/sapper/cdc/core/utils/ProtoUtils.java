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

package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.core.model.EngineType;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.entity.model.*;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.entity.schema.SchemaVersion;
import ai.sapper.hcdc.common.model.DFSTransaction;
import lombok.NonNull;

public class ProtoUtils {
    public static DFSTransaction buildTx(@NonNull HCdcTxId txId,
                                         @NonNull DFSTransaction.Operation op,
                                         boolean snapshot) {
        return DFSTransaction.newBuilder()
                .setId(txId.getId())
                .setSequence(txId.getSequence())
                .setRecordId(txId.getRecordId())
                .setOp(op)
                .setSnapshot(snapshot)
                .setTimestamp(System.currentTimeMillis())
                .build();
    }

    public static HCdcTxId fromTx(@NonNull DFSTransaction tx) {
        HCdcTxId id = new HCdcTxId();
        id.setType(EngineType.HDFS);
        id.setId(tx.getId());
        id.setSequence(tx.getSequence());
        id.setRecordId(tx.getRecordId());
        id.setSnapshot(tx.getSnapshot());
        return id;
    }

    public static String toString(@NonNull DFSTransaction tx) {
        return String.format("%s-%d-%d-%d",
                tx.getOp().name(),
                tx.getId(),
                tx.getSequence(),
                tx.getRecordId());
    }

    public static Transaction build(@NonNull HCdcTxId txId) {
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
