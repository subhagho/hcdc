package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.hcdc.io.FSBlock;
import ai.sapper.hcdc.io.FSFile;
import ai.sapper.hcdc.io.HCdcFsUtils;
import lombok.NonNull;

import java.io.IOException;

public class FileSystemHelper {
    public static FSBlock createBlockFile(@NonNull PathInfo dir,
                                          @NonNull DFSBlockState blockState,
                                          @NonNull FileSystem fs,
                                          @NonNull SchemaEntity entity) throws IOException {
        if (!fs.isDirectory(dir)) {
            throw new IOException(String.format("Specified path is not a directory. [path=%s]", dir.path()));
        }

        return HCdcFsUtils.create(dir, blockState, entity, fs);
    }

    public static FSFile createFile(@NonNull DFSFileState fileState,
                                    @NonNull FileSystem fs,
                                    @NonNull SchemaEntity entity) throws IOException {
        return HCdcFsUtils.create(fileState, entity, fs);
    }
}
