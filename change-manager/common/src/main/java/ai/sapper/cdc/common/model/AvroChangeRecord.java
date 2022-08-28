package ai.sapper.cdc.common.model;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

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
        txId = (int) record.get(AVRO_FIELD_TXID);
        if (txId < 0) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_OP));
        }
        String s = (String) record.get(AVRO_FIELD_OP);
        if (Strings.isNullOrEmpty(s)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_OP));
        }
        op = AvroChangeType.EChangeType.valueOf(s);
        String d = (String) record.get(AVRO_FIELD_TARGET_DOMAIN);
        if (Strings.isNullOrEmpty(d)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_TARGET_DOMAIN));
        }
        String e = (String) record.get(AVRO_FIELD_TARGET_ENTITY);
        if (Strings.isNullOrEmpty(e)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_TARGET_ENTITY));
        }
        targetEntity = new SchemaEntity(d, e);
        d = (String) record.get(AVRO_FIELD_SOURCE_DOMAIN);
        if (Strings.isNullOrEmpty(d)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_SOURCE_DOMAIN));
        }
        e = (String) record.get(AVRO_FIELD_SOURCE_ENTITY);
        if (Strings.isNullOrEmpty(e)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_SOURCE_ENTITY));
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

        GenericRecord wrapper = new GenericData.Record(schema);
        wrapper.put(AvroChangeRecord.AVRO_FIELD_TXID, txId);
        wrapper.put(AvroChangeRecord.AVRO_FIELD_OP, op.opCode());
        wrapper.put(AvroChangeRecord.AVRO_FIELD_TARGET_DOMAIN, targetEntity.getDomain());
        wrapper.put(AvroChangeRecord.AVRO_FIELD_TARGET_ENTITY, targetEntity.getEntity());
        wrapper.put(AvroChangeRecord.AVRO_FIELD_SOURCE_DOMAIN, sourceEntity.getDomain());
        wrapper.put(AvroChangeRecord.AVRO_FIELD_SOURCE_ENTITY, sourceEntity.getEntity());
        wrapper.put(AvroChangeRecord.AVRO_FIELD_TIMESTAMP, timestamp);
        wrapper.put(AvroChangeRecord.AVRO_FIELD_DATA, data);

        return wrapper;
    }
}
