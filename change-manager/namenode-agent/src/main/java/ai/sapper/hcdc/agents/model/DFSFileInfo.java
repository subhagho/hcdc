package ai.sapper.hcdc.agents.model;

import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.core.model.EFileType;
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
    private String schemaLocation;
    private SchemaVersion schemaVersion;

    public DFSFileInfo() {
    }

    public DFSFileInfo(@NonNull DFSFileInfo source) {
        this.namespace = source.namespace;
        this.hdfsPath = source.hdfsPath;
        this.inodeId = source.inodeId;
        this.fileType = source.fileType;
        this.schemaLocation = source.schemaLocation;
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
        if (!Strings.isNullOrEmpty(schemaLocation)) {
            builder.setSchemaLocation(schemaLocation);
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
        if (!Strings.isNullOrEmpty(schemaLocation)) {
            builder.setSchemaLocation(schemaLocation);
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
        if (file.hasSchemaLocation()) {
            schemaLocation = file.getSchemaLocation();
        }
        Preconditions.checkState(!Strings.isNullOrEmpty(namespace));
        Preconditions.checkState(!Strings.isNullOrEmpty(hdfsPath));

        return this;
    }
}
