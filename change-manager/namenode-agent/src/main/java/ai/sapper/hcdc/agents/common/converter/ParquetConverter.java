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

import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.model.AvroChangeType;
import ai.sapper.cdc.entity.model.ChangeEvent;
import ai.sapper.cdc.entity.model.DbSource;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.entity.types.DataType;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.apache.parquet.Strings;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ParquetConverter extends AvroBasedConverter {
    private static final String MAGIC_CODE = "PAR1";
    public static final String EXT = "parquet";

    public ParquetConverter(@NonNull DbSource source) {
        super(EFileType.PARQUET, source);
    }

    @Override
    public DataType<?> parseDataType(@NonNull String typeName,
                                     int jdbcType,
                                     long size,
                                     int... params) throws Exception {
        return null;
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public boolean canParse(@NonNull String path, EFileType fileType) throws IOException {
        if (fileType == EFileType.PARQUET) return true;
        String ext = FilenameUtils.getExtension(path);
        return (!Strings.isNullOrEmpty(ext) && ext.compareToIgnoreCase(EXT) == 0);
    }

    @Override
    public Response convert(@NonNull File source,
                            @NonNull File output,
                            @NonNull DFSFileState fileState,
                            @NonNull SchemaEntity schemaEntity,
                            @NonNull AvroChangeType.EChangeType op,
                            @NonNull HCdcTxId txId,
                            boolean snapshot) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        Configuration conf = new Configuration();
        conf.set(AvroReadSupport.READ_INT96_AS_FIXED, "true");
        try (ParquetReader<GenericRecord> reader = new AvroParquetReader(conf, new Path(source.toURI()))) {
            long count = 0;
            try {
                AvroEntitySchema schema = parseSchema(source, schemaEntity);

                try (FileOutputStream fos = new FileOutputStream(output)) {
                    while (true) {
                        GenericRecord record = reader.read();
                        if (record == null) break;
                        HCdcTxId tid = new HCdcTxId(txId);
                        tid.setRecordId(count);
                        ChangeEvent event = convert(schema,
                                record,
                                fileState.getFileInfo().getHdfsPath(),
                                op,
                                tid,
                                snapshot);
                        event.writeDelimitedTo(fos);
                        count++;
                    }
                }
                return new Response(output, count);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }

    private AvroEntitySchema parseSchema(File file,
                                         SchemaEntity schemaEntity) throws Exception {

        Configuration conf = new Configuration();
        conf.set(AvroReadSupport.READ_INT96_AS_FIXED, "true");
        try (ParquetFileReader reader =
                     ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI()), conf))) {
            MessageType pschema = reader.getFooter().getFileMetaData().getSchema();
            Schema schema = new AvroSchemaConverter(conf).convert(pschema);
            AvroEntitySchema avs = new AvroEntitySchema();
            avs.setSchemaEntity(schemaEntity);
            avs.setNamespace(schemaEntity.getDomain());
            avs.setName(schemaEntity.getEntity());
            avs.withSchema(schema, true);
            return schemaManager().checkAndSave(avs, schemaEntity);
        }
    }

    /**
     * @return
     */
    @Override
    public boolean supportsPartial() {
        return false;
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

    /**
     * @param reader
     * @return
     * @throws IOException
     */
    @Override
    public AvroEntitySchema extractSchema(@NonNull HDFSBlockReader reader,
                                          @NonNull DFSFileState fileState,
                                          @NonNull SchemaEntity schemaEntity) throws IOException {
        Preconditions.checkNotNull(schemaManager());
        try {
            AvroEntitySchema schema = hasSchema(fileState, schemaEntity);
            if (schema != null) {
                return schema;
            }
            DFSBlockState lastBlock = fileState.findLastBlock();
            if (lastBlock != null) {
                HDFSBlockData data = reader.read(lastBlock.getBlockId(),
                        lastBlock.getGenerationStamp(),
                        0L,
                        -1);

                if (data == null) {
                    throw new IOException(String.format("Error reading block from HDFS. [path=%s][block ID=%d]",
                            fileState.getFileInfo().getHdfsPath(), lastBlock.getBlockId()));
                }
                File tempf = PathUtils.getTempFileWithExt("parquet");
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
