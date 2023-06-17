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

package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.impl.azure.AzureFileSystem;
import ai.sapper.cdc.core.io.impl.local.LocalFileSystem;
import ai.sapper.cdc.core.io.impl.s3.S3FileSystem;
import ai.sapper.hcdc.common.model.DFSChangeData;
import lombok.NonNull;

public class HFSHelper {

    public static DFSChangeData.FileSystemCode fileSystemCode(@NonNull FileSystem fs) throws Exception {
        if (fs.getClass().equals(LocalFileSystem.class)) {
            return DFSChangeData.FileSystemCode.LOCAL;
        } else if (fs.getClass().equals(S3FileSystem.class)) {
            return DFSChangeData.FileSystemCode.S3;
        } else if (fs.getClass().equals(AzureFileSystem.class)) {
            return DFSChangeData.FileSystemCode.AZURE;
        }
        throw new Exception(String.format("FileSystem not recognized: [type=%s]", fs.getClass().getCanonicalName()));
    }
}
