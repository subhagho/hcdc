package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.cdc.common.schema.AvroUtils;
import ai.sapper.cdc.core.model.DFSFileState;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.schema.SchemaManager;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

    public Schema hasSchema(DFSFileState fileState, SchemaEntity schemaEntity) throws Exception {
        Schema schema = schemaManager().get(schemaEntity);
        if (schema == null) {
            if (!Strings.isNullOrEmpty(fileState.getSchemaLocation())) {
                schema = schemaManager().get(fileState.getSchemaLocation());
            }
        }
        return schema;
    }

    public GenericRecord wrap(Schema schema, GenericRecord record, int op, long txId) {
        Preconditions.checkNotNull(record);
        GenericRecord wrapper = new GenericData.Record(schema);
        wrapper.put(AvroUtils.AVRO_FIELD_TXID, txId);
        wrapper.put(AvroUtils.AVRO_FIELD_OP, op);
        wrapper.put(AvroUtils.AVRO_FIELD_TIMESTAMP, System.currentTimeMillis());
        wrapper.put(AvroUtils.AVRO_FIELD_DATA, record);

        return wrapper;
    }

    public abstract boolean canParse(@NonNull String path, EFileType fileType) throws IOException;

    public abstract File convert(@NonNull File source,
                                 @NonNull File output,
                                 @NonNull DFSFileState fileState,
                                 @NonNull SchemaEntity schemaEntity,
                                 long txId,
                                 int op) throws IOException;

    public abstract boolean supportsPartial();

    public abstract boolean detect(@NonNull String path, byte[] data, int length) throws IOException;

    public abstract Schema extractSchema(@NonNull HDFSBlockReader reader,
                                         @NonNull DFSFileState fileState,
                                         @NonNull SchemaEntity schemaEntity) throws IOException;
}
