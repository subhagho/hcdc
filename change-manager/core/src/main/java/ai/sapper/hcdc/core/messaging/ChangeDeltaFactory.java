package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.common.model.*;
import lombok.NonNull;

public class ChangeDeltaFactory {
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
}
