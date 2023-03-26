package ai.sapper.cdc.core.messaging;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.messaging.KafkaMessage;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessagingError;
import ai.sapper.cdc.core.utils.SchemaEntityHelper;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.cdc.core.utils.ProtoUtils;
import com.google.common.base.Strings;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import lombok.NonNull;

import java.util.UUID;

public class ChangeDeltaSerDe {
    public static MessageObject<String, DFSChangeDelta> createErrorTx(@NonNull String messageId,
                                                                      @NonNull DFSTransaction tnx,
                                                                      @NonNull DFSError.ErrorCode code,
                                                                      @NonNull String message,
                                                                      @NonNull SchemaEntity schemaEntity,
                                                                      DFSFile file) throws Exception {
        DFSError.Builder error = DFSError.newBuilder();
        error.setCode(code)
                .setMessage(message)
                .setTransaction(tnx);
        if (file == null) {
            DFSFile.Builder builder = DFSFile.newBuilder();
            builder.setInodeId(-1)
                    .setEntity(SchemaEntityHelper.proto(schemaEntity));
            file = builder.build();
        }
        error.setFile(file);
        MessageObject<String, DFSChangeDelta> m = create(error.build(),
                DFSError.class,
                schemaEntity, MessageObject.MessageMode.Error);
        m.correlationId(messageId);

        return m;
    }

    public static MessageObject<String, DFSChangeDelta> createIgnoreTx(@NonNull SchemaEntity schemaEntity,
                                                                       @NonNull DFSTransaction tnx,
                                                                       @NonNull MessageObject.MessageMode mode) throws Exception {
        DFSIgnoreTx ignoreTx = DFSIgnoreTx.newBuilder()
                .setOpCode(tnx.getOp().name())
                .setTransaction(tnx)
                .build();
        return create(ignoreTx, DFSIgnoreTx.class, schemaEntity, mode);
    }

    public static <T> MessageObject<String, DFSChangeDelta> create(@NonNull T data,
                                                                   @NonNull Class<? extends T> type,
                                                                   @NonNull SchemaEntity schemaEntity,
                                                                   @NonNull MessageObject.MessageMode mode) throws Exception {
        DFSChangeDelta delta = null;
        DFSChangeDelta.Builder builder = DFSChangeDelta.newBuilder();

        String key = null;
        String id = null;
        if (type.equals(DFSFileAdd.class) &&
                data instanceof DFSFileAdd) {
            id = create((DFSFileAdd) data, builder);
        } else if (type.equals(DFSFileAppend.class) &&
                data instanceof DFSFileAppend) {
            id = create((DFSFileAppend) data, builder);
        } else if (type.equals(DFSFileDelete.class) &&
                data instanceof DFSFileDelete) {
            id = create((DFSFileDelete) data, builder);
        } else if (type.equals(DFSBlockAdd.class) &&
                data instanceof DFSBlockAdd) {
            id = create((DFSBlockAdd) data, builder);
        } else if (type.equals(DFSBlockUpdate.class) &&
                data instanceof DFSBlockUpdate) {
            id = create((DFSBlockUpdate) data, builder);
        } else if (type.equals(DFSBlockTruncate.class) &&
                data instanceof DFSBlockTruncate) {
            id = create((DFSBlockTruncate) data, builder);
        } else if (type.equals(DFSFileClose.class) &&
                data instanceof DFSFileClose) {
            id = create((DFSFileClose) data, builder);
        } else if (type.equals(DFSFileRename.class) &&
                data instanceof DFSFileRename) {
            id = create((DFSFileRename) data, builder);
        } else if (type.equals(DFSIgnoreTx.class) &&
                data instanceof DFSIgnoreTx) {
            id = create((DFSIgnoreTx) data, builder);
        } else if (type.equals(DFSChangeData.class) &&
                data instanceof DFSChangeData) {
            id = create((DFSChangeData) data, builder);
        } else if (type.equals(DFSError.class) &&
                data instanceof DFSError) {
            id = create((DFSError) data, builder);
        } else if (type.equals(DFSSchemaChange.class) &&
                data instanceof DFSSchemaChange) {
            id = create((DFSSchemaChange) data, builder);
        } else {
            throw new MessagingError(String.format("Invalid Message DataType. [type=%s]", type.getCanonicalName()));
        }
        builder.setEntity(SchemaEntityHelper.proto(schemaEntity));
        delta = builder.build();
        key = getMessageKey(schemaEntity);
        if (Strings.isNullOrEmpty(id)) {
            id = UUID.randomUUID().toString();
        }
        MessageObject<String, DFSChangeDelta> message = new KafkaMessage<>();
        message.correlationId(ProtoUtils.toString(delta.getTx()));
        message.mode(mode);
        message.key(key);
        message.value(delta);
        if (DefaultLogger.LOGGER.isDebugEnabled()) {
            JsonFormat.Printer printer = JsonFormat.printer().preservingProtoFieldNames();
            StringBuilder mesg = new StringBuilder();
            mesg.append("Message: [").append(message.id()).append("]\n");
            mesg.append("Key: [").append(message.key()).append("]\n");
            mesg.append("Domain: [").append(schemaEntity).append("]\n");
            mesg.append("Data: [\n").append(printer.print((MessageOrBuilder) data)).append("\n]");

            DefaultLogger.LOGGER.debug(mesg.toString());
        }
        return message;
    }

    public static String getMessageKey(@NonNull SchemaEntity schemaEntity) {
        return String.format("%s::%s",
                schemaEntity.getDomain(),
                schemaEntity.getEntity());
    }

    public static String getMessageKey(@NonNull DFSSchemaEntity schemaEntity) {
        return String.format("%s::%s",
                schemaEntity.getDomain(),
                schemaEntity.getEntity());
    }

    public static MessageObject<String, DFSChangeDelta> update(@NonNull MessageObject<String, DFSChangeDelta> message,
                                                               @NonNull SchemaEntity schemaEntity,
                                                               @NonNull MessageObject.MessageMode mode) throws Exception {
        DFSChangeDelta delta = message.value();
        DFSSchemaEntity entity = SchemaEntityHelper.proto(schemaEntity);
        delta = delta.toBuilder()
                .setEntity(entity)
                .setTarget(entity)
                .build();
        MessageObject<String, DFSChangeDelta> m = new KafkaMessage<>(message);
        m.correlationId(message.correlationId());
        m.mode(mode);
        m.key(message.key());
        m.value(delta);

        return m;
    }

    public static String create(@NonNull DFSChangeData data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setDataChange(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSFileAdd data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setFileAdd(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSFileAppend data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setFileAppend(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSFileDelete data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setFileDelete(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSBlockAdd data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setBlockAdd(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSBlockUpdate data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setBlockUpdate(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSBlockTruncate data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setBlockTruncate(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSFileClose data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setFileClose(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSFileRename data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setFileRename(data);
        return String.valueOf(data.getSrcFile().getInodeId());
    }

    public static String create(@NonNull DFSIgnoreTx data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setIgnore(data);
        return String.valueOf(data.getFile().getInodeId());
    }

    public static String create(@NonNull DFSError data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setError(data);
        return UUID.randomUUID().toString();
    }

    public static String create(@NonNull DFSSchemaChange data,
                                @NonNull DFSChangeDelta.Builder builder) throws Exception {
        builder
                .setTimestamp(System.currentTimeMillis())
                .setTx(data.getTransaction())
                .setType(data.getClass().getCanonicalName())
                .setSchemaChange(data);
        return UUID.randomUUID().toString();
    }

    public static <T> T parse(@NonNull DFSChangeDelta delta,
                              @NonNull Class<? extends T> type) throws Exception {
        if (type.equals(DFSFileAdd.class)) {
            if (delta.hasFileAdd()) {
                return (T) delta.getFileAdd();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSFileAppend.class)) {
            if (delta.hasFileAppend()) {
                return (T) delta.getFileAppend();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSFileDelete.class)) {
            if (delta.hasFileDelete()) {
                return (T) delta.getFileDelete();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSBlockAdd.class)) {
            if (delta.hasBlockAdd()) {
                return (T) delta.getBlockAdd();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSBlockUpdate.class)) {
            if (delta.hasBlockUpdate()) {
                return (T) delta.getBlockUpdate();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSBlockTruncate.class)) {
            if (delta.hasBlockTruncate()) {
                return (T) delta.getBlockTruncate();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSFileClose.class)) {
            if (delta.hasFileClose()) {
                return (T) delta.getFileClose();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSFileRename.class)) {
            if (delta.hasFileRename()) {
                return (T) delta.getFileRename();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSIgnoreTx.class)) {
            if (delta.hasIgnore()) {
                return (T) delta.getIgnore();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSChangeData.class)) {
            if (delta.hasDataChange()) {
                return (T) delta.getDataChange();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSError.class)) {
            if (delta.hasError()) {
                return (T) delta.getError();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else if (type.equals(DFSSchemaChange.class)) {
            if (delta.hasSchemaChange()) {
                return (T) delta.getSchemaChange();
            } else {
                throw new MessagingError(
                        String.format("Invalid Message: Type not found. [type=%s]",
                                type.getCanonicalName()));
            }
        } else {
            throw new MessagingError(String.format("Invalid Message DataType. [type=%s]", type.getCanonicalName()));
        }
    }
}
