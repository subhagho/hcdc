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

package ai.sapper.cdc.core.model.dfs;

import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.entity.schema.SchemaVersion;
import ai.sapper.hcdc.common.model.DFSFile;
import ai.sapper.hcdc.common.model.DFSSchemaEntity;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class DFSFileInfo {
    private String namespace;
    private String hdfsPath;
    private long inodeId;
    private EFileType fileType = EFileType.UNKNOWN;
    private String schemaURI;
    private SchemaVersion schemaVersion;

    public DFSFileInfo() {
    }

    public DFSFileInfo(@NonNull DFSFileInfo source) {
        this.namespace = source.namespace;
        this.hdfsPath = source.hdfsPath;
        this.inodeId = source.inodeId;
        this.fileType = source.fileType;
        this.schemaURI = source.schemaURI;
    }

    public DFSFile proto() {
        Preconditions.checkState(!Strings.isNullOrEmpty(namespace));
        Preconditions.checkState(!Strings.isNullOrEmpty(hdfsPath));
        DFSFile.Builder builder = DFSFile.newBuilder();
        DFSSchemaEntity.Builder entity = DFSSchemaEntity.newBuilder();
        entity.setDomain(namespace)
                .setEntity(hdfsPath);
        builder.setEntity(entity)
                .setInodeId(inodeId)
                .setFileType(fileType.name());
        if (!Strings.isNullOrEmpty(schemaURI)) {
            builder.setSchemaURI(schemaURI);
        }
        return builder.build();
    }

    public DFSFile proto(@NonNull String targetPath) {
        Preconditions.checkState(!Strings.isNullOrEmpty(namespace));
        DFSFile.Builder builder = DFSFile.newBuilder();
        DFSSchemaEntity.Builder entity = DFSSchemaEntity.newBuilder();
        entity.setDomain(namespace)
                .setEntity(targetPath);
        builder.setEntity(entity)
                .setInodeId(inodeId)
                .setFileType(fileType.name());
        if (!Strings.isNullOrEmpty(schemaURI)) {
            builder.setSchemaURI(schemaURI);
        }
        return builder.build();
    }

    public DFSFileInfo parse(@NonNull DFSFile file) {
        namespace = file.getEntity().getDomain();
        hdfsPath = file.getEntity().getEntity();
        inodeId = file
                .getInodeId();
        if (file.hasFileType()) {
            fileType = EFileType.valueOf(file.getFileType());
        }
        if (file.hasSchemaURI()) {
            schemaURI = file.getSchemaURI();
        }
        Preconditions.checkState(!Strings.isNullOrEmpty(namespace));
        Preconditions.checkState(!Strings.isNullOrEmpty(hdfsPath));

        return this;
    }
}
