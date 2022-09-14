package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileState;
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
        if (file.hasSchemaLocation()) {
            fileState.getFileInfo().setSchemaLocation(file.getSchemaLocation());
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
        if (file.hasSchemaLocation()) {
            replicaState.getFileInfo().setSchemaLocation(file.getSchemaLocation());
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
        if (!Strings.isNullOrEmpty(fileState.getFileInfo().getSchemaLocation())) {
            replicaState.getFileInfo().setSchemaLocation(fileState.getFileInfo().getSchemaLocation());
            updated = true;
        }
        return updated;
    }

    public static DFSFile update(@NonNull DFSFile file,
                                 @NonNull String schemaLocation) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaLocation));
        DFSFile.Builder builder = file.toBuilder();
        builder.setSchemaLocation(schemaLocation);

        return builder.build();
    }
}
