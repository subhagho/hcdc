/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.hcdc.agents.common.converter;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.model.AvroChangeType;
import ai.sapper.cdc.entity.model.ChangeEvent;
import ai.sapper.cdc.entity.model.DbSource;
import ai.sapper.cdc.entity.schema.AvroUtils;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.entity.schema.SchemaEvolutionValidator;
import ai.sapper.cdc.entity.schema.SchemaHelper;
import ai.sapper.cdc.entity.types.DataType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.slf4j.event.Level;

import java.io.*;
import java.util.List;
import java.util.Map;

public class JsonConverter extends AvroBasedConverter {
    public static final String EXT = "json";

    public JsonConverter(@NonNull DbSource source) {
        super(EFileType.JSON, source);
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

    @Override
    public Response convert(@NonNull File source,
                            @NonNull Writer writer,
                            @NonNull DFSFileState fileState,
                            @NonNull SchemaEntity schemaEntity,
                            AvroChangeType.@NonNull EChangeType op,
                            @NonNull HCdcTxId txId) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        try {
            AvroEntitySchema schema = hasSchema(fileState, schemaEntity);
            if (schema == null) {
                schema = parseSchema(source, schemaEntity);
            }
            if (schema == null) {
                throw new IOException(
                        String.format("Error generating Avro Schema. [entity=%s]", schemaEntity.toString()));
            }
            long count = 0;
            long size = 0;
            FileOutputStream fos = (FileOutputStream) writer.getOutputStream();
            try (BufferedReader br = new BufferedReader(new FileReader(source))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (Strings.isNullOrEmpty(line)) continue;
                    GenericRecord record = AvroUtils.jsonToAvroRecord(line, schema.getSchema());
                    HCdcTxId tid = new HCdcTxId(txId);
                    tid.setRecordId(count);
                    ChangeEvent event = convert(schema,
                            record,
                            fileState.getFileInfo().getHdfsPath(),
                            op,
                            tid);
                    size += event.getSerializedSize();
                    event.writeDelimitedTo(fos);
                    count++;
                }
            }
            writer.flush();
            writer.commit(true);
            DefaultLogger.info(String.format("[%s] Data size = %d", writer.inode().getPathInfo(), size));
            return new Response(writer.inode().getPathInfo(), count);
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

    private AvroEntitySchema parseSchema(File file,
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
                            DefaultLogger.warn(
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
            AvroEntitySchema avs = new AvroEntitySchema();
            avs.setSchemaEntity(schemaEntity);
            avs.setNamespace(schemaEntity.getDomain());
            avs.setName(schemaEntity.getEntity());
            avs.withSchema(schema, true);
            return schemaManager().checkAndSave(avs, schemaEntity);
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
    public AvroEntitySchema extractSchema(@NonNull HDFSBlockReader reader,
                                          @NonNull DFSFileState fileState,
                                          @NonNull SchemaEntity schemaEntity) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        try {
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
                return parseSchema(tempf, schemaEntity);
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

    @Override
    public DataType<?> parseDataType(@NonNull String typeName,
                                     int jdbcType,
                                     long size,
                                     int... params) throws Exception {
        return null;
    }
}
