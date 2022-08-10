package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.hcdc.agents.common.FormatConverter;
import ai.sapper.hcdc.common.schema.SchemaHelper;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileType;
import ai.sapper.hcdc.core.model.HDFSBlockData;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.apache.parquet.Strings;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonConverter implements FormatConverter {
    public static final String EXT = "json";

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
    public File convert(@NonNull File source, @NonNull File output) throws IOException {
        return null;
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

    /**
     * @return
     */
    @Override
    public EFileType fileType() {
        return EFileType.JSON;
    }

    /**
     * @param reader
     * @param fileState
     * @return
     * @throws IOException
     */
    @Override
    public Schema extractSchema(@NonNull HDFSBlockReader reader,
                              @NonNull DFSFileState fileState) throws IOException {
        try {
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
                Schema schema = null;
                ObjectMapper mapper = new ObjectMapper();
                try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data.data().array())) {
                    try (InputStreamReader streamReader = new InputStreamReader(byteStream)) {
                        try (BufferedReader buffered = new BufferedReader(streamReader)) {
                            String line = null;
                            StringBuilder builder = new StringBuilder();
                            while ((line = buffered.readLine()) != null) {
                                line = line.trim();
                                if (Strings.isNullOrEmpty(line)) continue;
                                Object jObj = tryParseLine(line, mapper);
                                if (jObj != null) {
                                    schema = getSchema(jObj, mapper);
                                }
                            }
                        }
                    }
                }
                return schema;
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
