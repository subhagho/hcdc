package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.model.*;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import lombok.NonNull;

import java.util.UUID;

public class ChangeDeltaSerDe {
    public static <T> MessageObject<String, DFSChangeDelta> createErrorTx(@NonNull String namespace,
                                                                          @NonNull String messageId,
                                                                          @NonNull DFSTransaction tnx,
                                                                          @NonNull DFSError.ErrorCode code,
                                                                          @NonNull String message,
                                                                          DFSFile file) throws Exception {
        DFSError.Builder error = DFSError.newBuilder();
        error.setCode(code)
                .setMessage(message)
                .setTransaction(tnx);
        if (file != null) {
            error.setFile(file);
        }
        MessageObject<String, DFSChangeDelta> m = create(namespace,
                error.build(),
                DFSError.class,
                null, null, MessageObject.MessageMode.Error);
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
        return create(namespace, ignoreTx, DFSIgnoreTx.class, null, null, mode);
    }

    public static <T> MessageObject<String, DFSChangeDelta> create(@NonNull String namespace,
                                                                   @NonNull T data,
                                                                   @NonNull Class<? extends T> type,
                                                                   String domain,
                                                                   String entity,
                                                                   @NonNull MessageObject.MessageMode mode) throws Exception {
        DFSChangeDelta delta = null;
        DFSChangeDelta.Builder builder = DFSChangeDelta.newBuilder();
        String key = null;
        String id = null;
        if (type.equals(DFSAddFile.class)) {
            id = create(namespace, (DFSAddFile) data, builder);
        } else if (type.equals(DFSAppendFile.class)) {
            id = create(namespace, (DFSAppendFile) data, builder);
        } else if (type.equals(DFSDeleteFile.class)) {
            id = create(namespace, (DFSDeleteFile) data, builder);
        } else if (type.equals(DFSAddBlock.class)) {
            id = create(namespace, (DFSAddBlock) data, builder);
        } else if (type.equals(DFSUpdateBlocks.class)) {
            id = create(namespace, (DFSUpdateBlocks) data, builder);
        } else if (type.equals(DFSTruncateBlock.class)) {
            id = create(namespace, (DFSTruncateBlock) data, builder);
        } else if (type.equals(DFSCloseFile.class)) {
            id = create(namespace, (DFSCloseFile) data, builder);
        } else if (type.equals(DFSRenameFile.class)) {
            id = create(namespace, (DFSRenameFile) data, builder);
        } else if (type.equals(DFSIgnoreTx.class)) {
            id = create(namespace, (DFSIgnoreTx) data, builder);
        } else if (type.equals(DFSChangeData.class)) {
            id = create(namespace, (DFSChangeData) data, builder);
        } else if (type.equals(DFSError.class)) {
            id = create(namespace, (DFSError) data, builder);
        } else {
            throw new MessagingError(String.format("Invalid Message DataType. [type=%s]", type.getCanonicalName()));
        }
        if (!Strings.isNullOrEmpty(domain)) {
            builder.setDomain(domain);
        }
        if (!Strings.isNullOrEmpty(entity)) {
            builder.setEntityName(entity);
        }
        delta = builder.build();
        key = delta.getEntity();
        if (Strings.isNullOrEmpty(id)) {
            id = UUID.randomUUID().toString();
        }
        MessageObject<String, DFSChangeDelta> message = new KafkaMessage<>();
        message.correlationId(String.valueOf(delta.getTxId()));
        message.mode(mode);
        message.key(key);
        message.value(delta);
        if (DefaultLogger.LOG.isDebugEnabled()) {
            JsonFormat.Printer printer = JsonFormat.printer().preservingProtoFieldNames();
            StringBuilder mesg = new StringBuilder();
            mesg.append("Message: [").append(message.id()).append("]\n");
            mesg.append("Key: [").append(message.key()).append("]\n");
            mesg.append("Domain: [").append(delta.getDomain()).append(":").append(delta.getEntityName()).append("]\n");
            mesg.append("Data: [\n").append(printer.print((MessageOrBuilder) data)).append("\n]");

            DefaultLogger.LOG.debug(mesg.toString());
        }
        return message;
    }

    public static MessageObject<String, DFSChangeDelta> update(@NonNull MessageObject<String, DFSChangeDelta> message,
                                                               @NonNull SchemaEntity schemaEntity,
                                                               @NonNull MessageObject.MessageMode mode) throws Exception {
        DFSChangeDelta delta = message.value();
        delta = delta.toBuilder()
                .setEntityName(schemaEntity.getEntity())
                .setDomain(schemaEntity.getDomain())
                .build();
        MessageObject<String, DFSChangeDelta> m = new KafkaMessage<>(message);
        m.correlationId(message.correlationId());
        m.mode(mode);
        m.key(message.key());
        m.value(delta);

        return m;
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSChangeData data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSAddFile data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSAppendFile data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSDeleteFile data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSAddBlock data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSUpdateBlocks data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSTruncateBlock data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSCloseFile data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSRenameFile data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(data.getSrcFile().getPath())
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getSrcFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSIgnoreTx data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity(String.format("IGNORE:%s", data.getTransaction().getTransactionId()))
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull String namespace,
                                @NonNull DFSError data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setNamespace(namespace)
                .setTimestamp(System.currentTimeMillis())
                .setTxId(String.valueOf(data.getTransaction().getTransactionId()))
                .setEntity("")
                .setType(data.getClass().getCanonicalName())
                .setBody(data.toByteString());
        return UUID.randomUUID().toString();
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
        } else if (type.compareTo(DFSChangeData.class.getCanonicalName()) == 0) {
            return DFSChangeData.parseFrom(changeDelta.getBody());
        } else if (type.compareTo(DFSError.class.getCanonicalName()) == 0) {
            return DFSError.parseFrom(changeDelta.getBody());
        } else {
            throw new MessagingError(String.format("Invalid Message Type. [type=%s]", type));
        }
    }
}
