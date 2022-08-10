package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.common.model.SchemaEntity;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileType;
import ai.sapper.hcdc.core.schema.SchemaManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.hadoop.hdfs.HDFSBlockReader;

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

    public abstract boolean canParse(@NonNull String path, EFileType fileType) throws IOException;

    public abstract File convert(@NonNull File source,
                                 @NonNull File output,
                                 @NonNull DFSFileState fileState,
                                 @NonNull SchemaEntity schemaEntity) throws IOException;

    public abstract boolean supportsPartial();

    public abstract boolean detect(@NonNull String path, byte[] data, int length) throws IOException;

    public abstract Schema extractSchema(@NonNull HDFSBlockReader reader,
                                         @NonNull DFSFileState fileState,
                                         @NonNull SchemaEntity schemaEntity) throws IOException;
}
