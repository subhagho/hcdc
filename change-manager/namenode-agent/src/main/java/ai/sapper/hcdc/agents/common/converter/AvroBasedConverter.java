package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.schema.SchemaHelper;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.entity.BasicDataTypeReaders;
import ai.sapper.cdc.entity.DataType;
import ai.sapper.cdc.entity.DataTypeReader;
import ai.sapper.cdc.entity.DecimalType;
import ai.sapper.cdc.entity.avro.AvroDataTypeReaders;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.jdbc.DbEntitySchema;
import ai.sapper.cdc.entity.model.*;
import ai.sapper.cdc.entity.schema.SchemaField;
import ai.sapper.hcdc.agents.common.FormatConverter;
import ai.sapper.cdc.core.utils.ProtoUtils;
import com.google.protobuf.ByteString;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class AvroBasedConverter extends FormatConverter {
    public AvroBasedConverter(@NonNull EFileType fileType, @NonNull DbSource source) {
        super(fileType, source);
    }


    public ChangeEvent convert(@NonNull AvroEntitySchema schema,
                               @NonNull GenericRecord record,
                               @NonNull String sourcePath,
                               @NonNull AvroChangeType.EChangeType op,
                               @NonNull BaseTxId txId,
                               boolean snapshot) throws Exception {
        Transaction tnx = ProtoUtils.build(txId);
        DbChangeRecord change = buildChangeRecord(schema.getSchemaEntity(),
                schema,
                record,
                sourcePath,
                op);
        ChangeEvent.Builder builder = ChangeEvent.newBuilder()
                .setType(ChangeEvent.EventType.DATA)
                .setSource(source())
                .setTnx(tnx)
                .setEntity(change.getEntity())
                .setChangeRecord(change)
                .setSnapshot(snapshot);
        return builder.build();
    }


    public DbChangeRecord buildChangeRecord(@NonNull SchemaEntity schemaEntity,
                                            @NonNull AvroEntitySchema schema,
                                            @NonNull GenericRecord record,
                                            @NonNull String sourcePath,
                                            @NonNull AvroChangeType.EChangeType op) throws Exception {
        DbChangeRecord.Builder builder = DbChangeRecord.newBuilder();
        DbEntity entity = ProtoUtils.build(schemaEntity, sourcePath);
        builder.setEntity(entity);
        DbChangeRecord.ChangeType ct = ProtoUtils.changeType(op);
        if (ct == null) {
            throw new Exception(String.format("Invalid change type. [op=%s]", op.name()));
        }
        builder.setType(ct);
        if (ct == DbChangeRecord.ChangeType.CREATE
                || ct == DbChangeRecord.ChangeType.UPDATE) {
            builder.setAfter(buildRecord(schema, record));
        } else {
            builder.setAfter(buildRecord(schema, record));
        }
        Version v = ProtoUtils.build(schema.getVersion());
        builder.setVersion(v);
        return builder.build();
    }

    public DbRecord buildRecord(@NonNull AvroEntitySchema schema,
                                @NonNull GenericRecord record) throws Exception {
        DbRecord.Builder builder = DbRecord.newBuilder();
        Schema avroSchema = record.getSchema();
        for (Schema.Field field : avroSchema.getFields()) {
            String name = SchemaHelper.checkFieldName(field.name());
            name = name.toUpperCase();
            SchemaField sf = schema.get(name);
            if (sf != null) {
                Object value = record.get(field.name());
                DbColumnData column = buildColumn(sf, value);
                builder.addColumns(column);
            }
        }
        return builder.build();
    }

    public DbColumnData buildColumn(SchemaField field,
                                    Object value) throws Exception {
        DbColumnData.Builder builder = DbColumnData.newBuilder();
        DataType<?> type = field.getDataType();
        builder.setName(field.getName())
                .setType(AvroEntitySchema.getProtoType(type))
                .setPosition(field.getPosition());
        DbPrimitiveValue.Builder vb = DbPrimitiveValue.newBuilder();
        if (value == null) {
            vb.setNull(NullColumnData.newBuilder().setNull(true).build());
        } else {
            updateValue(vb, type, value);
        }
        builder.setData(DbValue.newBuilder().setPrimitive(vb.build()));
        return builder.build();
    }


    @Override
    public void updateValue(DbPrimitiveValue.@NonNull Builder vb,
                            @NonNull DataType<?> type,
                            Object value) throws Exception {
        if (type.equals(AvroEntitySchema.TIME_MILLIS)) {
            long ts = BasicDataTypeReaders.INTEGER_READER.read(value);
            LongColumnData d = LongColumnData.newBuilder()
                    .setData(ts)
                    .build();
            vb.setLong(d);
            return;
        } else if (type.equals(AvroEntitySchema.TIME_MICROS)) {
            long ts = BasicDataTypeReaders.LONG_READER.read(value);
            ts = microToMilliSeconds(ts);
            LongColumnData d = LongColumnData.newBuilder()
                    .setData(ts)
                    .build();
            vb.setLong(d);
            return;
        } else if (type.equals(DbEntitySchema.DATE)) {
            long ts = BasicDataTypeReaders.LONG_READER.read(value);
            ts = dateToDateTime(ts);
            LongColumnData d = LongColumnData.newBuilder()
                    .setData(ts)
                    .build();
            vb.setLong(d);
            return;
        } else if (type.equals(AvroEntitySchema.TIMESTAMP_MILLIS)) {
            long ts = BasicDataTypeReaders.LONG_READER.read(value);
            LongColumnData d = LongColumnData.newBuilder()
                    .setData(ts)
                    .build();
            vb.setLong(d);
            return;
        } else if (type.equals(AvroEntitySchema.TIMESTAMP_MICROS)) {
            long ts = BasicDataTypeReaders.LONG_READER.read(value);
            ts = microToMilliSeconds(ts);
            LongColumnData d = LongColumnData.newBuilder()
                    .setData(ts)
                    .build();
            vb.setLong(d);
            return;
        } else if (type.isType(DbEntitySchema.DECIMAL)) {
            ByteBuffer buff = BasicDataTypeReaders.BYTES_READER.read(value);
            DecimalType<BigDecimal> dt = (DecimalType<BigDecimal>) type;
            byte[] bytes = new byte[buff.remaining()];
            buff.duplicate().get(bytes);
            BigIntColumnData bic = BigIntColumnData.newBuilder()
                    .setValue(ByteString.copyFrom(bytes))
                    .build();
            BigDecColumnData bdc = BigDecColumnData.newBuilder()
                    .setValue(bic)
                    .setScale(dt.getScale())
                    .setPrecision(dt.getPrecision())
                    .build();
            vb.setDecimal(bdc);
            return;
        }
        if (updatePrimitiveValue(vb, type, value)) return;
        DataTypeReader<?> reader = AvroDataTypeReaders.getReader(type);
        if (reader == null) {
            throw new Exception(String.format("DataType reader not found. [type=%s]", type.getName()));
        }
        Object v = reader.read(value);
        if (v != null) {
            String json = JSONUtils.asString(v, v.getClass());
            JsonColumnData d = JsonColumnData.newBuilder()
                    .setType(v.getClass().getCanonicalName())
                    .setData(ByteString.copyFrom(json, StandardCharsets.UTF_8))
                    .build();
            vb.setJson(d);
        } else {
            throw new Exception(String.format("Error extracting type value. [type=%s]", type.getName()));
        }
    }
}
