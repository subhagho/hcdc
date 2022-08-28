package ai.sapper.hcdc.utils;

import ai.sapper.cdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroDataWriter extends OutputDataWriter<List<String>> {
    protected AvroDataWriter(@NonNull String path,
                             @NonNull String filename,
                             @NonNull FileSystem fs) {
        super(path, filename, fs, EOutputFormat.Avro);
    }

    @Override
    public void write(String name,
                      @NonNull Map<String, Integer> header,
                      @NonNull List<List<String>> records) throws IOException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        String file = String.format("%s/%s", path(), filename());
        File f = new File(file);
        if (f.exists()) {
            f.delete();
        }
        try {
            SchemaBuilder.FieldAssembler<Schema> record = SchemaBuilder.record("record")
                    .namespace("ai.sapper.hcdc")
                    .fields();
            for (String key : header.keySet()) {
                record.nullableString(key, "");
            }
            Schema schema = record.endRecord();
            Path path = new Path(path());
            if (!fs().exists(path)) {
                fs().mkdirs(path);
            }
            path = new Path(file);

            List<GenericData.Record> data = convert(schema, records, header);

            writeToAvro(schema, data, path);
        } catch (Exception ex) {
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(ex));
            throw new IOException(ex);
        }
    }

    @Override
    public boolean doUpload() {
        return false;
    }

    public void writeToAvro(Schema schema, List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {
        Configuration conf = new Configuration();
        OutputStream out = fs().create(fileToWrite);
        DatumWriter<GenericRecord> datumWriter = new
                GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datumWriter)) {
            // for compression
            //dataFileWriter.setCodec(CodecFactory.snappyCodec());
            writer.create(schema, out);
            for (GenericData.Record record : recordsToWrite) {
                writer.append(record);
            }
        }
    }

    private List<GenericData.Record> convert(Schema schema, List<List<String>> records, Map<String, Integer> header) throws Exception {
        Map<Integer, String> reverse = new HashMap<>(header.size());
        for (String key : header.keySet()) {
            reverse.put(header.get(key), key);
        }
        List<GenericData.Record> data = new ArrayList<>();
        for (List<String> r : records) {
            GenericData.Record record = new GenericData.Record(schema);
            for (int ii = 0; ii < r.size(); ii++) {
                String name = reverse.get(ii);
                Preconditions.checkState(!Strings.isNullOrEmpty(name));
                record.put(name, r.get(ii));
            }
            data.add(record);
        }
        return data;
    }

    @Override
    public void close() throws IOException {

    }
}
