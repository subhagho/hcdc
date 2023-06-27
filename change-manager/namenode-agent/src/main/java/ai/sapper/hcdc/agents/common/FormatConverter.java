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

package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.entity.ValueParser;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import ai.sapper.cdc.entity.model.AvroChangeType;
import ai.sapper.cdc.entity.model.DbPrimitiveValue;
import ai.sapper.cdc.entity.model.DbSource;
import ai.sapper.cdc.entity.schema.EntitySchema;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.entity.types.DataType;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.apache.parquet.Strings;

import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class FormatConverter extends ValueParser {
    private HCdcSchemaManager schemaManager;
    private final EFileType fileType;
    private final DbSource source;

    public FormatConverter(@NonNull EFileType fileType,
                           @NonNull DbSource source) {
        this.fileType = fileType;
        this.source = source;
    }

    public FormatConverter withSchemaManager(@NonNull HCdcSchemaManager schemaManager) {
        this.schemaManager = schemaManager;
        return this;
    }

    public AvroEntitySchema hasSchema(@NonNull DFSFileState fileState, SchemaEntity schemaEntity) throws Exception {
        if (schemaEntity != null) {
            AvroEntitySchema schema = schemaManager().getSchema(schemaEntity, AvroEntitySchema.class);
            if (schema == null) {
                if (!Strings.isNullOrEmpty(fileState.getFileInfo().getSchemaURI())) {
                    schema = schemaManager().getSchema(schemaEntity,
                            fileState.getFileInfo().getSchemaURI(),
                            AvroEntitySchema.class);
                }
            }
            return schema;
        }
        return null;
    }

    public abstract boolean canParse(@NonNull String path, EFileType fileType) throws IOException;

    public abstract Response convert(@NonNull File source,
                                     @NonNull Writer writer,
                                     @NonNull DFSFileState fileState,
                                     @NonNull SchemaEntity schemaEntity,
                                     @NonNull AvroChangeType.EChangeType op,
                                     @NonNull HCdcTxId txId) throws IOException;

    public abstract boolean supportsPartial();

    public abstract boolean detect(@NonNull String path, byte[] data, int length) throws IOException;

    public abstract EntitySchema extractSchema(@NonNull HDFSBlockReader reader,
                                               @NonNull DFSFileState fileState,
                                               @NonNull SchemaEntity schemaEntity) throws IOException;

    public abstract void updateValue(@NonNull DbPrimitiveValue.Builder vb,
                                     @NonNull DataType<?> type,
                                     Object value) throws Exception;

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class Response {
        private PathInfo path;
        private long recordCount;

        public Response(@NonNull PathInfo path, long recordCount) {
            this.path = path;
            this.recordCount = recordCount;
        }
    }
}
