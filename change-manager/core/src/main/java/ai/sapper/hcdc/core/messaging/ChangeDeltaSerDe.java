package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Preconditions;
import lombok.NonNull;

public class ChangeDeltaSerDe {
    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSAddFile data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSAppendFile data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSDeleteFile data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSAddBlock data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSUpdateBlocks data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSTruncateBlock data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSCloseFile data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSRenameFile data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getSrcFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSIgnoreTx data) throws Exception {
        return DFSChangeDelta.newBuilder()
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity("")
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString())
                .build();
    }

    public static Object parse(@NonNull DFSChangeDelta changeDelta) throws Exception {
        Preconditions.checkArgument(changeDelta.hasType());
        String type = changeDelta.getType();
        if (type.compareTo(DFSAddFile.class.getCanonicalName()) == 0) {
            return DFSAddFile.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSAppendFile.class.getCanonicalName()) == 0) {
            return DFSAppendFile.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSDeleteFile.class.getCanonicalName()) == 0) {
            return DFSDeleteFile.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSAddBlock.class.getCanonicalName()) == 0) {
            return DFSAddBlock.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSUpdateBlocks.class.getCanonicalName()) == 0) {
            return DFSUpdateBlocks.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSTruncateBlock.class.getCanonicalName()) == 0) {
            return DFSTruncateBlock.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSCloseFile.class.getCanonicalName()) == 0) {
            return DFSCloseFile.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSRenameFile.class.getCanonicalName()) == 0) {
            return DFSRenameFile.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSIgnoreTx.class.getCanonicalName()) == 0) {
            return DFSIgnoreTx.parseFrom(changeDelta.getBody());
        } else {
            throw new MessagingError(String.format("Invalid Message Type. [type=%s]", type));
        }
    }
}
