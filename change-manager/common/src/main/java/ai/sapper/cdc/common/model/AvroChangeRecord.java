package ai.sapper.cdc.common.model;

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
    public static final String AVRO_FIELD_DOMAIN = "domain";
    public static final String AVRO_FIELD_ENTITY = "entity";
    public static final String AVRO_FIELD_DATA = "data";

    private long txId = -1;
    private AvroChangeType.EChangeType op;
    private long timestamp;
    private SchemaEntity schemaEntity;
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
        String d = (String) record.get(AVRO_FIELD_DOMAIN);
        if (Strings.isNullOrEmpty(d)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_DOMAIN));
        }
        String e = (String) record.get(AVRO_FIELD_ENTITY);
        if (Strings.isNullOrEmpty(e)) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_ENTITY));
        }
        data = (GenericRecord) record.get(AVRO_FIELD_DATA);
        if (data == null) {
            throw new IOException(String.format("Data Error: missing field. [field=%s]", AVRO_FIELD_DATA));
        }
        return this;
    }

    public GenericRecord toAvro(@NonNull Schema schema) throws IOException {
        GenericRecord wrapper = new GenericData.Record(schema);
        wrapper.put(AvroChangeRecord.AVRO_FIELD_TXID, txId);
        wrapper.put(AvroChangeRecord.AVRO_FIELD_OP, op.opCode());
        wrapper.put(AvroChangeRecord.AVRO_FIELD_DOMAIN, schemaEntity.getDomain());
        wrapper.put(AvroChangeRecord.AVRO_FIELD_ENTITY, schemaEntity.getEntity());
        wrapper.put(AvroChangeRecord.AVRO_FIELD_TIMESTAMP, timestamp);
        wrapper.put(AvroChangeRecord.AVRO_FIELD_DATA, data);

        return wrapper;
    }
}
