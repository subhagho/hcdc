package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import ai.sapper.hcdc.io.FSBlock;
import ai.sapper.hcdc.io.FSFile;
import ai.sapper.hcdc.io.HCDCFsUtils;
import lombok.NonNull;

import java.io.IOException;

public class FileSystemHelper {
    public static FSBlock createBlockFile(@NonNull PathInfo dir,
                                          @NonNull DFSBlockState blockState,
                                          @NonNull CDCFileSystem fs,
                                          @NonNull SchemaEntity entity) throws IOException {
        if (!dir.isDirectory()) {
            throw new IOException(String.format("Specified path is not a directory. [path=%s]", dir.path()));
        }

        return HCDCFsUtils.create(dir, blockState, entity, fs);
    }

    public static FSFile createFile(@NonNull DFSFileState fileState,
                                    @NonNull CDCFileSystem fs,
                                    @NonNull SchemaEntity entity) throws IOException {
        return HCDCFsUtils.create(fileState, entity, fs);
    }
}
