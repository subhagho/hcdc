package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.schema.AvroSchema;
import ai.sapper.cdc.common.schema.AvroUtils;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.hcdc.agents.common.FormatConverter;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.apache.parquet.Strings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AvroConverter extends FormatConverter {
    private static final String MAGIC_CODE = "Obj1";
    public static final String EXT = "avro";

    public AvroConverter() {
        super(EFileType.AVRO);
    }

    /**
     * @param path
     * @param fileType
     * @return
     * @throws IOException
     */
    @Override
    public boolean canParse(@NonNull String path, EFileType fileType) throws IOException {
        if (fileType == EFileType.AVRO) return true;
        String ext = FilenameUtils.getExtension(path);
        return (!Strings.isNullOrEmpty(ext) && ext.compareToIgnoreCase(EXT) == 0);
    }

    /**
     * @param source
     * @param output
     * @throws IOException
     */
    @Override
    public Response convert(@NonNull File source,
                            @NonNull File output,
                            @NonNull DFSFileState fileState,
                            @NonNull SchemaEntity schemaEntity,
                            long txId,
                            @NonNull AvroChangeType.EChangeType op) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        try {
            long count = 0;
            AvroSchema schema = parseSchema(source, schemaEntity);
            Schema wrapper = AvroUtils.createSchema(schema.getSchema());
            final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(wrapper);
            try (DataFileWriter<GenericRecord> fos = new DataFileWriter<>(writer)) {
                fos.setCodec(CodecFactory.snappyCodec());
                fos.create(wrapper, output);
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema.getSchema());
                try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(source, reader)) {
                    while (dataFileReader.hasNext()) {
                        GenericRecord record = dataFileReader.next();
                        if (record == null) break;
                        GenericRecord wrapped = wrap(wrapper,
                                schemaEntity,
                                fileState.getFileInfo().getNamespace(),
                                fileState.getFileInfo().getHdfsPath(),
                                record, op, txId);
                        fos.append(wrapped);
                        count++;
                    }
                }
            }
            return new Response(output, count);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * @return
     */
    @Override
    public boolean supportsPartial() {
        return true;
    }

    /**
     * @param data
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public boolean detect(@NonNull String path, byte[] data, int length) throws IOException {
        if (canParse(path, null)) return true;
        if (data != null && length >= 4) {
            String m = new String(data, 0, 4, StandardCharsets.UTF_8);
            return m.compareTo(MAGIC_CODE) == 0;
        }
        return false;
    }

    private AvroSchema parseSchema(File file,
                                   SchemaEntity schemaEntity) throws Exception {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            Schema schema = dataFileReader.getSchema();
            AvroSchema avs = new AvroSchema();
            avs.withSchema(schema);
            return schemaManager().checkAndSave(avs, schemaEntity);
        }
    }

    /**
     * @param reader
     * @return
     * @throws IOException
     */
    @Override
    public AvroSchema extractSchema(@NonNull HDFSBlockReader reader,
                                   @NonNull DFSFileState fileState,
                                   @NonNull SchemaEntity schemaEntity) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        try {
            AvroSchema schema = hasSchema(fileState, schemaEntity);
            if (schema != null) {
                return schema;
            }
            DFSBlockState firstBlock = fileState.findFirstBlock();
            if (firstBlock != null) {
                HDFSBlockData data = reader.read(firstBlock.getBlockId(),
                        firstBlock.getGenerationStamp(),
                        0L,
                        -1);

                if (data == null) {
                    throw new IOException(String.format("Error reading block from HDFS. [path=%s][block ID=%d]",
                            fileState.getFileInfo().getHdfsPath(), firstBlock.getBlockId()));
                }
                File tempf = PathUtils.getTempFileWithExt("avro");
                try (FileOutputStream fos = new FileOutputStream(tempf)) {
                    fos.write(data.data().array());
                    fos.flush();
                }
                return parseSchema(tempf, schemaEntity);
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
