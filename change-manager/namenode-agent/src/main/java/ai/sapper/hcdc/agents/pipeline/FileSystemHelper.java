package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.cdc.core.io.FSBlock;
import ai.sapper.cdc.core.io.FSFile;
import ai.sapper.cdc.core.io.HCDCFileSystem;
import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.cdc.core.model.DFSBlockState;
import ai.sapper.cdc.core.model.DFSFileState;
import lombok.NonNull;

import java.io.IOException;

public class FileSystemHelper {
    public static FSBlock createBlockFile(@NonNull PathInfo dir,
                                          @NonNull DFSBlockState blockState,
                                          @NonNull HCDCFileSystem fs,
                                          @NonNull SchemaEntity entity) throws IOException {
        if (!dir.isDirectory()) {
            throw new IOException(String.format("Specified path is not a directory. [path=%s]", dir.path()));
        }

        return fs.create(dir, blockState, entity);
    }

    public static FSFile createFile(@NonNull DFSFileState fileState,
                                    @NonNull HCDCFileSystem fs,
                                    @NonNull SchemaEntity entity) throws IOException {
        return fs.create(fileState, entity);
    }
}
