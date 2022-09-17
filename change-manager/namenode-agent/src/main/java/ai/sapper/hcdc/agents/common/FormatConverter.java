package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.model.AvroChangeRecord;
import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.schema.AvroSchema;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.schema.SchemaManager;
import ai.sapper.hcdc.agents.model.DFSFileState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hdfs.HDFSBlockReader;
import org.apache.parquet.Strings;

import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class FormatConverter {
    private SchemaManager schemaManager;
    private EFileType fileType;

    public FormatConverter(@NonNull EFileType fileType) {
        this.fileType = fileType;
    }

    public FormatConverter withSchemaManager(@NonNull SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
        return this;
    }

    public AvroSchema hasSchema(DFSFileState fileState, SchemaEntity schemaEntity) throws Exception {
        if (schemaEntity != null) {
            AvroSchema schema = schemaManager().get(schemaEntity);
            if (schema == null) {
                if (!Strings.isNullOrEmpty(fileState.getFileInfo().getSchemaLocation())) {
                    schema = schemaManager().get(schemaEntity, fileState.getFileInfo().getSchemaLocation());
                }
            }
            return schema;
        }
        return null;
    }

    public GenericRecord wrap(@NonNull Schema schema,
                              @NonNull SchemaEntity schemaEntity,
                              @NonNull String namespace,
                              @NonNull String hdfsPath,
                              @NonNull GenericRecord record,
                              @NonNull AvroChangeType.EChangeType op,
                              long txId) throws IOException {
        Preconditions.checkNotNull(record);
        AvroChangeRecord avro = new AvroChangeRecord()
                .txId(txId)
                .timestamp(System.currentTimeMillis())
                .op(op)
                .targetEntity(schemaEntity)
                .sourceEntity(new SchemaEntity(namespace, hdfsPath))
                .data(record);

        return avro.toAvro(schema);
    }

    public abstract boolean canParse(@NonNull String path, EFileType fileType) throws IOException;

    public abstract Response convert(@NonNull File source,
                                     @NonNull File output,
                                     @NonNull DFSFileState fileState,
                                     @NonNull SchemaEntity schemaEntity,
                                     long txId,
                                     @NonNull AvroChangeType.EChangeType op) throws IOException;

    public abstract boolean supportsPartial();

    public abstract boolean detect(@NonNull String path, byte[] data, int length) throws IOException;

    public abstract AvroSchema extractSchema(@NonNull HDFSBlockReader reader,
                                             @NonNull DFSFileState fileState,
                                             @NonNull SchemaEntity schemaEntity) throws IOException;

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
