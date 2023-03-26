package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.entity.DataType;
import ai.sapper.cdc.entity.ValueParser;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.model.DbPrimitiveValue;
import ai.sapper.cdc.entity.model.DbSource;
import ai.sapper.cdc.entity.schema.EntitySchema;
import ai.sapper.cdc.entity.schema.SchemaManager;
import ai.sapper.hcdc.agents.model.DFSFileState;
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
    private SchemaManager schemaManager;
    private final EFileType fileType;
    private final DbSource source;

    public FormatConverter(@NonNull EFileType fileType,
                           @NonNull DbSource source) {
        this.fileType = fileType;
        this.source = source;
    }

    public FormatConverter withSchemaManager(@NonNull SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
        return this;
    }

    public AvroEntitySchema hasSchema(DFSFileState fileState, SchemaEntity schemaEntity) throws Exception {
        if (schemaEntity != null) {
            AvroEntitySchema schema = schemaManager().get(schemaEntity);
            if (schema == null) {
                if (!Strings.isNullOrEmpty(fileState.getFileInfo().getSchemaLocation())) {
                    schema = schemaManager().get(schemaEntity, fileState.getFileInfo().getSchemaLocation());
                }
            }
            return schema;
        }
        return null;
    }

    public abstract boolean canParse(@NonNull String path, EFileType fileType) throws IOException;

    public abstract Response convert(@NonNull File source,
                                     @NonNull File output,
                                     @NonNull DFSFileState fileState,
                                     @NonNull SchemaEntity schemaEntity,
                                     @NonNull AvroChangeType.EChangeType op,
                                     @NonNull BaseTxId txId,
                                     boolean snapshot) throws IOException;

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
        private File file;
        private long recordCount;

        public Response(@NonNull File file, long recordCount) {
            this.file = file;
            this.recordCount = recordCount;
        }
    }
}
