package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileType;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.hadoop.hdfs.HDFSBlockReader;

import java.io.File;
import java.io.IOException;

public interface FormatConverter {
    boolean canParse(@NonNull String path, EFileType fileType) throws IOException;

    File convert(@NonNull File source, @NonNull File output) throws IOException;

    boolean supportsPartial();

    boolean detect(@NonNull String path, byte[] data, int length) throws IOException;

    EFileType fileType();

    Schema extractSchema(@NonNull HDFSBlockReader reader,
                         @NonNull DFSFileState fileState) throws IOException;
}
