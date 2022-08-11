package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.hcdc.agents.common.FormatConverter;
import ai.sapper.hcdc.common.model.SchemaEntity;
import ai.sapper.hcdc.common.schema.AvroUtils;
import ai.sapper.hcdc.common.schema.SchemaHelper;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileType;
import ai.sapper.hcdc.core.model.HDFSBlockData;
import ai.sapper.hcdc.core.schema.SchemaEvolutionValidator;
import ai.sapper.hcdc.core.schema.SchemaManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.apache.log4j.Level;
import org.apache.parquet.Strings;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class JsonConverter extends FormatConverter {
    public static final String EXT = "json";

    public JsonConverter() {
        super(EFileType.JSON);
    }

    /**
     * @param path
     * @param fileType
     * @return
     * @throws IOException
     */
    @Override
    public boolean canParse(@NonNull String path, EFileType fileType) throws IOException {
        if (fileType == EFileType.JSON) return true;
        String ext = FilenameUtils.getExtension(path);
        return (!Strings.isNullOrEmpty(ext) && ext.compareToIgnoreCase(EXT) == 0);
    }

    /**
     * @param source
     * @param output
     * @return
     * @throws IOException
     */
    @Override
    public File convert(@NonNull File source,
                        @NonNull File output,
                        @NonNull DFSFileState fileState,
                        @NonNull SchemaEntity schemaEntity) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        try {
            Schema schema = hasSchema(fileState, schemaEntity);
            if (schema == null) {
                schema = parseSchema(source, fileState, schemaEntity);
            }
            final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            try (DataFileWriter<GenericRecord> fos = new DataFileWriter<>(writer)) {
                fos.create(schema, output);
                try (BufferedReader br = new BufferedReader(new FileReader(source))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (Strings.isNullOrEmpty(line)) continue;
                        GenericRecord record = AvroUtils.jsonToAvroRecord(line, schema);
                        fos.append(record);
                    }
                }
            }
            return output;
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
     * @param path
     * @param data
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public boolean detect(@NonNull String path, byte[] data, int length) throws IOException {
        return canParse(path, null);
    }

    private Schema parseSchema(File file,
                               DFSFileState fileState,
                               SchemaEntity schemaEntity) throws Exception {
        Schema schema = null;
        ObjectMapper mapper = new ObjectMapper();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (Strings.isNullOrEmpty(line)) continue;
                Object jObj = tryParseLine(line, mapper);
                if (jObj != null) {
                    Schema _schema = getSchema(jObj, mapper);
                    if (schema == null) {
                        schema = _schema;
                    } else {
                        List<SchemaEvolutionValidator.Message> messages
                                = SchemaEvolutionValidator.checkBackwardCompatibility(_schema, schema, schema.getName());
                        Level maxLevel = Level.ALL;
                        for (SchemaEvolutionValidator.Message message : messages) {
                            if (message.getLevel().isGreaterOrEqual(maxLevel)) {
                                maxLevel = message.getLevel();
                            }
                        }

                        if (maxLevel.isGreaterOrEqual(Level.ERROR)) {
                            DefaultLogger.LOG.warn(
                                    String.format("Found incompatible schema. [schema=%s][entity=%s]",
                                            _schema.toString(true), schemaEntity.toString()));
                        } else {
                            schema = _schema;
                        }
                    }
                }
            }
        }
        if (schema != null) {
            schemaManager().checkAndSave(schema, schemaEntity);
        }
        return schema;
    }

    /**
     * @param reader
     * @param fileState
     * @return
     * @throws IOException
     */
    @Override
    public Schema extractSchema(@NonNull HDFSBlockReader reader,
                                @NonNull DFSFileState fileState,
                                @NonNull SchemaEntity schemaEntity) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        try {
            Schema schema = hasSchema(fileState, schemaEntity);
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
                            fileState.getHdfsFilePath(), firstBlock.getBlockId()));
                }
                File tempf = PathUtils.getTempFileWithExt("json");
                try (FileOutputStream fos = new FileOutputStream(tempf)) {
                    fos.write(data.data().array());
                    fos.flush();
                }
                return parseSchema(tempf, fileState, schemaEntity);
            }

            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private Schema getSchema(Object jObj, ObjectMapper mapper) throws Exception {
        if (jObj instanceof Map) {
            Map<String, Object> jMap = (Map<String, Object>) jObj;
            return SchemaHelper.JsonToAvroSchema.convert(jMap, "default", "", mapper);
        }
        return null;
    }

    private Object tryParseLine(String line, ObjectMapper mapper) {
        try {
            return mapper.readValue(line, Object.class);
        } catch (Exception ex) {
            return null;
        }
    }
}
