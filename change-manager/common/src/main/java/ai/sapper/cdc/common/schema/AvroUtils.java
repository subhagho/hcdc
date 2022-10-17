package ai.sapper.cdc.common.schema;

import ai.sapper.cdc.common.model.AvroChangeRecord;
import ai.sapper.cdc.common.utils.DefaultLogger;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.*;

public class AvroUtils {
    private static final String SCHEMA_REPLACE = "__SCHEMA__";
    private static final String AVRO_SCHEMA_WRAPPER = String.format("{\n" +
                    "\t\"type\" : \"record\",\n" +
                    "\t\"namespace\" : \"ai.sapper.cdc.deltas\", \n" +
                    "\t\"name\" : \"ChangeDelta\", \n" +
                    "\t\"fields\" : [\n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : \"long\" }, \n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : \"int\" }, \n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : \"string\" }, \n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : \"string\" }, \n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : [\"null\", \"int\"], \"default\": null }, \n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : \"string\" }, \n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : \"string\" }, \n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : \"long\" }, \n" +
                    "\t\t{ \"name\" : \"%s\", \"type\" : %s \n }\n] \n" +
                    "}",
            AvroChangeRecord.AVRO_FIELD_TXID,
            AvroChangeRecord.AVRO_FIELD_OP,
            AvroChangeRecord.AVRO_FIELD_TARGET_DOMAIN,
            AvroChangeRecord.AVRO_FIELD_TARGET_ENTITY,
            AvroChangeRecord.AVRO_FIELD_TARGET_GROUP,
            AvroChangeRecord.AVRO_FIELD_SOURCE_DOMAIN,
            AvroChangeRecord.AVRO_FIELD_SOURCE_ENTITY,
            AvroChangeRecord.AVRO_FIELD_TIMESTAMP,
            AvroChangeRecord.AVRO_FIELD_DATA,
            SCHEMA_REPLACE);

    public static Schema createSchema(@NonNull Schema schema) throws Exception {
        String scStr = AVRO_SCHEMA_WRAPPER.replace(SCHEMA_REPLACE, schema.toString(false));
        return new Schema.Parser().parse(scStr);
    }

    public static GenericRecord jsonToAvroRecord(@NonNull String json, @NonNull Schema schema) throws IOException {
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            InputStream stream = new ByteArrayInputStream(json.getBytes());
            DataInputStream din = new DataInputStream(stream);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            return reader.read(null, decoder);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public static byte[] jsonToAvro(String json, String schemaStr) throws IOException {
        Schema schema = new Schema.Parser().parse(schemaStr);
        return jsonToAvro(json, schema);
    }

    public static byte[] jsonToAvro(String json, Schema schema) throws IOException {
        InputStream stream = null;
        GenericDatumWriter<GenericRecord> writer = null;
        Encoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            stream = new ByteArrayInputStream(json.getBytes());
            output = new ByteArrayOutputStream();
            DataInputStream din = new DataInputStream(stream);
            writer = new GenericDatumWriter<GenericRecord>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            encoder = EncoderFactory.get().binaryEncoder(output, null);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.write(datum, encoder);
            }
            encoder.flush();
            return output.toByteArray();
        } finally {
            if (stream != null)
                stream.close();
        }
    }

    public static String avroToJson(byte[] avro, String schemaStr) throws IOException {
        Schema schema = new Schema.Parser().parse(schemaStr);
        return avroToJson(avro, schema);
    }

    public static String avroToJson(byte[] avro, Schema schema) throws IOException {
        boolean pretty = false;
        GenericDatumReader<GenericRecord> reader = null;
        JsonEncoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            reader = new GenericDatumReader<GenericRecord>(schema);
            InputStream input = new ByteArrayInputStream(avro);
            output = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
            Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.write(datum, encoder);
            }
            encoder.flush();
            output.flush();
            return new String(output.toByteArray());
        } finally {
            try {
                if (output != null) output.close();
            } catch (Exception e) {
                DefaultLogger.LOGGER.error(e.getLocalizedMessage());
            }
        }
    }
}
