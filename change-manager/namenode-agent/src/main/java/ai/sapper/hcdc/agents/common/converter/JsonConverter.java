package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.schema.*;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.hcdc.agents.common.FormatConverter;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.slf4j.event.Level;

import java.io.*;
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
    public Response convert(@NonNull File source,
                            @NonNull File output,
                            @NonNull DFSFileState fileState,
                            @NonNull SchemaEntity schemaEntity,
                            long txId,
                            @NonNull AvroChangeType.EChangeType op) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        try {
            AvroSchema schema = hasSchema(fileState, schemaEntity);
            if (schema == null) {
                schema = parseSchema(source, fileState, schemaEntity);
            }
            if (schema == null) {
                throw new IOException(
                        String.format("Error generating Avro Schema. [entity=%s]", schemaEntity.toString()));
            }
            long count = 0;
            Schema wrapper = AvroUtils.createSchema(schema.getSchema());
            final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema.getSchema());
            try (DataFileWriter<GenericRecord> fos = new DataFileWriter<>(writer)) {
                fos.setCodec(CodecFactory.snappyCodec());
                fos.create(schema.getSchema(), output);
                try (BufferedReader br = new BufferedReader(new FileReader(source))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (Strings.isNullOrEmpty(line)) continue;
                        GenericRecord record = AvroUtils.jsonToAvroRecord(line, schema.getSchema());
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

    private AvroSchema parseSchema(File file,
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
                    Schema _schema = getSchema(jObj, false);
                    if (schema == null) {
                        schema = _schema;
                    } else {
                        List<SchemaEvolutionValidator.Message> messages
                                = SchemaEvolutionValidator.checkBackwardCompatibility(_schema, schema, schema.getName());
                        Level maxLevel = Level.DEBUG;
                        for (SchemaEvolutionValidator.Message message : messages) {
                            if (DefaultLogger.isGreaterOrEqual(message.getLevel(), maxLevel)) {
                                maxLevel = message.getLevel();
                            }
                        }

                        if (DefaultLogger.isGreaterOrEqual(maxLevel, Level.ERROR)) {
                            DefaultLogger.LOGGER.warn(
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
            AvroSchema avs = new AvroSchema();
            return schemaManager()
                    .checkAndSave(avs.withSchema(schema), schemaEntity);
        }
        return null;
    }

    /**
     * @param reader
     * @param fileState
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

    @SuppressWarnings("unchecked")
    private Schema getSchema(Object jObj, boolean deep) throws Exception {
        if (jObj instanceof Map) {
            Map<String, Object> jMap = (Map<String, Object>) jObj;
            return SchemaHelper.JsonToAvroSchema.convert(jMap, "default", "", deep);
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
