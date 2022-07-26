package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.hcdc.agents.common.FormatConverter;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

public class ParquetConverter implements FormatConverter {
    public static final String EXT = "parquet";

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public boolean canParse(@NonNull File path) throws IOException {
        String ext = FilenameUtils.getExtension(path.getName());
        return (!Strings.isNullOrEmpty(ext) && ext.compareToIgnoreCase(EXT) == 0);
    }

    /**
     * @param source
     * @param output
     * @throws IOException
     */
    @Override
    public void convert(@NonNull File source, @NonNull File output) throws IOException {
        ParquetReader<GenericRecord> reader = AvroParquetReader.genericRecordReader(new Path(source.toURI()));
        try {
            Schema schema = getSchema(source);
            final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            try (DataFileWriter<GenericRecord> fos = new DataFileWriter(writer)) {
                fos.create(schema, output);
                while (true) {
                    GenericRecord record = reader.read();
                    if (record == null) break;
                    fos.append(record);
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private Schema getSchema(File file) throws Exception {
        Configuration conf = new Configuration();
        try (ParquetFileReader reader =
                     ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI()), conf))) {
            MessageType pschema = reader.getFooter().getFileMetaData().getSchema();
            return new AvroSchemaConverter(conf).convert(pschema);
        }
    }

    /**
     * @return
     */
    @Override
    public boolean supportsPartial() {
        return false;
    }
}
