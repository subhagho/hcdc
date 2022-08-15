package ai.sapper.cdc.common.schema;

import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.*;

public class AvroUtils {
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
            }
        }
    }
}
