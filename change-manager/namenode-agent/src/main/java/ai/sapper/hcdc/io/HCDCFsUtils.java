package ai.sapper.hcdc.io;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import lombok.NonNull;

import java.io.IOException;

public class HCDCFsUtils {

    public static FSFile create(@NonNull DFSFileState fileState,
                                @NonNull SchemaEntity entity,
                                @NonNull FileSystem fs) throws IOException {
        return new FSFile(fileState, entity.getDomain(), fs, true);
    }

    public static FSBlock create(@NonNull PathInfo dir,
                                 @NonNull DFSBlockState blockState,
                                 @NonNull SchemaEntity entity,
                                 @NonNull FileSystem fs) throws IOException {
        return new FSBlock(blockState, dir, fs, entity.getDomain(), true);
    }

    public static FSFile get(@NonNull DFSFileState fileState,
                             @NonNull SchemaEntity entity,
                             @NonNull FileSystem fs) throws IOException {
        return new FSFile(fileState, entity.getDomain(), fs, false);
    }

    public static FSBlock get(@NonNull PathInfo dir,
                              @NonNull DFSBlockState blockState,
                              @NonNull SchemaEntity entity,
                              @NonNull FileSystem fs) throws IOException {
        return new FSBlock(blockState, dir, fs, entity.getDomain(), false);
    }
}
