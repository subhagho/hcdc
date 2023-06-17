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

package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.entity.schema.SchemaEntity;
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
