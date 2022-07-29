package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.common.model.SchemaEntity;
import ai.sapper.hcdc.core.io.FSBlock;
import ai.sapper.hcdc.core.io.FSFile;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import lombok.NonNull;

import java.io.IOException;

public class FileSystemHelper {
    public static FSBlock createBlockFile(@NonNull PathInfo dir,
                                          @NonNull DFSBlockState blockState,
                                          @NonNull FileSystem fs,
                                          @NonNull SchemaEntity entity) throws IOException {
        if (!dir.isDirectory()) {
            throw new IOException(String.format("Specified path is not a directory. [path=%s]", dir.path()));
        }

        return fs.create(dir, blockState, entity);
    }

    public static FSFile createFile(@NonNull DFSFileState fileState,
                                    @NonNull FileSystem fs,
                                    @NonNull SchemaEntity entity) throws IOException {
        return fs.create(fileState, entity);
    }
}
