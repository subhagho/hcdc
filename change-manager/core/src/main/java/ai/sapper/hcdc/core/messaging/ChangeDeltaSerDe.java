package ai.sapper.hcdc.core.messaging;

import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Preconditions;
import lombok.NonNull;

public class ChangeDeltaSerDe {
    public static <T> MessageObject<String, DFSChangeDelta> createErrorTx(@NonNull String namespace,
                                                                          @NonNull String messageId,
                                                                          @NonNull DFSTransaction tnx,
                                                                          @NonNull DFSError.ErrorCode code,
                                                                          @NonNull String message) throws Exception {
        DFSError error = DFSError.newBuilder()
                .setCode(code)
                .setMessage(message)
                .setTransaction(tnx)
                .build();
        MessageObject<String, DFSChangeDelta> m = create(namespace, error, DFSError.class, MessageObject.MessageMode.Error);
        m.correlationId(messageId);

        return m;
    }

    public static <T> MessageObject<String, DFSChangeDelta> createIgnoreTx(@NonNull String namespace,
                                                                           @NonNull DFSTransaction tnx,
                                                                           @NonNull MessageObject.MessageMode mode) throws Exception {
        DFSIgnoreTx ignoreTx = DFSIgnoreTx.newBuilder()
                .setOpCode(tnx.getOp().name())
                .setTransaction(tnx)
                .build();
        return create(namespace, ignoreTx, DFSIgnoreTx.class, mode);
    }

    public static <T> MessageObject<String, DFSChangeDelta> create(@NonNull String namespace,
                                                                   @NonNull Object data,
                                                                   @NonNull Class<? extends T> type,
                                                                   @NonNull MessageObject.MessageMode mode) throws Exception {
        DFSChangeDelta delta = null;
        String key = null;
        if (type.equals(DFSAddFile.class)) {
            delta = create(namespace, (DFSAddFile) data);
            key = delta.getEntity();
        } else if (type.equals(DFSAppendFile.class)) {
            delta = create(namespace, (DFSAppendFile) data);
            key = delta.getEntity();
        } else if (type.equals(DFSDeleteFile.class)) {
            delta = create(namespace, (DFSDeleteFile) data);
            key = delta.getEntity();
        } else if (type.equals(DFSAddBlock.class)) {
            delta = create(namespace, (DFSAddBlock) data);
            key = delta.getEntity();
        } else if (type.equals(DFSUpdateBlocks.class)) {
            delta = create(namespace, (DFSUpdateBlocks) data);
            key = delta.getEntity();
        } else if (type.equals(DFSTruncateBlock.class)) {
            delta = create(namespace, (DFSTruncateBlock) data);
            key = delta.getEntity();
        } else if (type.equals(DFSCloseFile.class)) {
            delta = create(namespace, (DFSCloseFile) data);
            key = delta.getEntity();
        } else if (type.equals(DFSRenameFile.class)) {
            delta = create(namespace, (DFSRenameFile) data);
            key = delta.getEntity();
        } else if (type.equals(DFSIgnoreTx.class)) {
            delta = create(namespace, (DFSIgnoreTx) data);
            key = String.format("IGNORE:%s", delta.getTxId());
        } else {
            throw new MessagingError(String.format("Invalid Message DataType. [type=%s]", type.getCanonicalName()));
        }
        MessageObject<String, DFSChangeDelta> message = new KafkaMessage<>();
        message.id(String.format("%s:%s:%s", namespace, mode.name(), delta.getTxId()));
        message.correlationId(key);
        message.mode(mode);
        message.key(key);
        message.value(delta);

        return message;
    }

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

    public static DFSChangeDelta create(@NonNull String namespace,
                                        @NonNull DFSError data) throws Exception {
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
