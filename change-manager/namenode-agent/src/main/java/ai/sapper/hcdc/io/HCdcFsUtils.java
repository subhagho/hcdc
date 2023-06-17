/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
