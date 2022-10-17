package ai.sapper.cdc.common.model;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.ReflectionUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public class AvroChangeRecord {
    public static final String AVRO_FIELD_TXID = "txId";
    public static final String AVRO_FIELD_OP = "op";
    public static final String AVRO_FIELD_TIMESTAMP = "timestamp";
    public static final String AVRO_FIELD_TARGET_DOMAIN = "targetDomain";
    public static final String AVRO_FIELD_TARGET_ENTITY = "targetEntity";
    public static final String AVRO_FIELD_TARGET_GROUP = "group";
    public static final String AVRO_FIELD_SOURCE_DOMAIN = "sourceDomain";
    public static final String AVRO_FIELD_SOURCE_ENTITY = "sourceEntity";
    public static final String AVRO_FIELD_DATA = "data";

    private long txId = -1;
    private AvroChangeType.EChangeType op;
    private long timestamp;
    private SchemaEntity targetEntity;
    private SchemaEntity sourceEntity;
    private GenericRecord data;

    public AvroChangeRecord parse(@NonNull GenericRecord record) throws IOException {
        txId = (long) record.get(AVRO_FIELD_TXID);
        if (txId < 0) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_OP));
        }
        int o = (int) record.get(AVRO_FIELD_OP);
        op = AvroChangeType.EChangeType.values()[o];
        String d = record.get(AVRO_FIELD_TARGET_DOMAIN).toString();
        if (Strings.isNullOrEmpty(d)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_TARGET_DOMAIN));
        }
        String e = record.get(AVRO_FIELD_TARGET_ENTITY).toString();
        if (Strings.isNullOrEmpty(e)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_TARGET_ENTITY));
        }
        targetEntity = new SchemaEntity(d, e);
        d = record.get(AVRO_FIELD_SOURCE_DOMAIN).toString();
        if (Strings.isNullOrEmpty(d)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_SOURCE_DOMAIN));
        }
        e = record.get(AVRO_FIELD_SOURCE_ENTITY).toString();
        if (Strings.isNullOrEmpty(e)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_SOURCE_ENTITY));
        }
        Object g = record.get(AVRO_FIELD_TARGET_GROUP);
        if (g != null) {
            if (ReflectionUtils.isNumericType(g.getClass())) {
                targetEntity.setGroup((int) g);
            } else {
                throw new IOException(
                        String.format("Invalid Group datatype. [type=%s]", g.getClass().getCanonicalName()));
            }
        }
        sourceEntity = new SchemaEntity(d, e);
        data = (GenericRecord) record.get(AVRO_FIELD_DATA);
        if (data == null) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_DATA));
        }
        return this;
    }

    public GenericRecord toAvro(@NonNull Schema schema) throws IOException {
        Preconditions.checkNotNull(op);
        Preconditions.checkNotNull(targetEntity);
        Preconditions.checkNotNull(sourceEntity);
        Preconditions.checkNotNull(data);

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(AvroChangeRecord.AVRO_FIELD_TXID, txId);
        builder.set(AvroChangeRecord.AVRO_FIELD_OP, op.opCode());
        builder.set(AvroChangeRecord.AVRO_FIELD_TARGET_DOMAIN, targetEntity.getDomain());
        builder.set(AvroChangeRecord.AVRO_FIELD_TARGET_ENTITY, targetEntity.getEntity());
        builder.set(AvroChangeRecord.AVRO_FIELD_TARGET_GROUP, targetEntity.getGroup());
        builder.set(AvroChangeRecord.AVRO_FIELD_SOURCE_DOMAIN, sourceEntity.getDomain());
        builder.set(AvroChangeRecord.AVRO_FIELD_SOURCE_ENTITY, sourceEntity.getEntity());
        builder.set(AvroChangeRecord.AVRO_FIELD_TIMESTAMP, timestamp);
        builder.set(AvroChangeRecord.AVRO_FIELD_DATA,
                GenericData.get().deepCopy(data.getSchema(), data));

        return builder.build();
    }

    public static byte[] serialize(@NonNull GenericRecord record) throws Exception {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Schema schema = record.getSchema();
            EncoderFactory encoderfactory = new EncoderFactory();
            BinaryEncoder encoder = encoderfactory.binaryEncoder(out, null);
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            writer.write(record, encoder);
            encoder.flush();

            return out.toByteArray();
        }
    }
}
