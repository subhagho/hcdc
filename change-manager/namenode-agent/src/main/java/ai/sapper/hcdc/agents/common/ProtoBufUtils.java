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

package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.dfs.DFSFileReplicaState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.hcdc.common.model.DFSFile;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;

public class ProtoBufUtils {
    public static boolean update(@NonNull DFSFileState fileState, @NonNull DFSFile file) throws Exception {
        Preconditions.checkArgument(fileState.getFileInfo().getInodeId() == file.getInodeId());
        boolean updated = false;
        if (file.hasFileType()) {
            EFileType fileType = EFileType.parse(file.getFileType());
            if (fileType != null && fileType != EFileType.UNKNOWN) {
                fileState.getFileInfo().setFileType(fileType);
                updated = true;
            }
        }
        if (file.hasSchemaURI()) {
            fileState.getFileInfo().setSchemaURI(file.getSchemaURI());
            updated = true;
        }
        return updated;
    }

    public static boolean update(@NonNull DFSFileReplicaState replicaState, @NonNull DFSFile file) throws Exception {
        boolean updated = false;
        Preconditions.checkArgument(replicaState.getFileInfo().getInodeId() == file.getInodeId());
        if (file.hasFileType()) {
            EFileType fileType = EFileType.parse(file.getFileType());
            if (fileType != null && fileType != EFileType.UNKNOWN) {
                replicaState.getFileInfo().setFileType(fileType);
                updated = true;
            }
        }
        if (file.hasSchemaURI()) {
            replicaState.getFileInfo().setSchemaURI(file.getSchemaURI());
            updated = true;
        }
        return updated;
    }

    public static boolean update(@NonNull DFSFileState fileState, @NonNull DFSFileReplicaState replicaState) {
        boolean updated = false;
        if (fileState.getFileInfo().getFileType() != null
                && fileState.getFileInfo().getFileType() != EFileType.UNKNOWN) {
            replicaState.getFileInfo().setFileType(fileState.getFileInfo().getFileType());
            updated = true;
        }
        if (!Strings.isNullOrEmpty(fileState.getFileInfo().getSchemaURI())) {
            replicaState.getFileInfo().setSchemaURI(fileState.getFileInfo().getSchemaURI());
            updated = true;
        }
        return updated;
    }

    public static DFSFile update(@NonNull DFSFile file,
                                 @NonNull String schemaLocation) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaLocation));
        DFSFile.Builder builder = file.toBuilder();
        builder.setSchemaURI(schemaLocation);

        return builder.build();
    }
}
