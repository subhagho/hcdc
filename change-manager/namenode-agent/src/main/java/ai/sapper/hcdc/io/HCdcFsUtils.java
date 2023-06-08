package ai.sapper.hcdc.io;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.DirectoryInode;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import lombok.NonNull;

import java.io.IOException;

public class HCdcFsUtils {

    public static FSFile create(@NonNull DFSFileState fileState,
                                @NonNull SchemaEntity entity,
                                @NonNull FileSystem fs) throws IOException {
        return new FSFile(fileState, entity.getDomain(), fs);
    }

    public static FSBlock create(@NonNull PathInfo dir,
                                 @NonNull DFSBlockState blockState,
                                 @NonNull SchemaEntity entity,
                                 @NonNull FileSystem fs) throws IOException {
        DirectoryInode node = (DirectoryInode) fs.getInode(dir);
        if (node == null) {
            throw new IOException(String.format("Parent directory not found. [dir=%s]", dir.toString()));
        }
        return new FSBlock(blockState, node, fs, entity.getDomain(), true);
    }

    public static FSBlock create(@NonNull DirectoryInode dir,
                                 @NonNull DFSBlockState blockState,
                                 @NonNull SchemaEntity entity,
                                 @NonNull FileSystem fs) throws IOException {
        return new FSBlock(blockState, dir, fs, entity.getDomain(), true);
    }

    public static FSFile get(@NonNull DFSFileState fileState,
                             @NonNull SchemaEntity entity,
                             @NonNull FileSystem fs) throws IOException {
        return new FSFile(fileState, entity.getDomain(), fs);
    }

    public static FSBlock get(@NonNull PathInfo dir,
                              @NonNull DFSBlockState blockState,
                              @NonNull SchemaEntity entity,
                              @NonNull FileSystem fs) throws IOException {
        DirectoryInode node = (DirectoryInode) fs.getInode(dir);
        if (node == null) {
            return null;
        }
        return new FSBlock(blockState, node, fs, entity.getDomain(), false);
    }

    public static FSBlock get(@NonNull DirectoryInode dir,
                              @NonNull DFSBlockState blockState,
                              @NonNull SchemaEntity entity,
                              @NonNull FileSystem fs) throws IOException {
        return new FSBlock(blockState, dir, fs, entity.getDomain(), false);
    }
}
