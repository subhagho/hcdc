package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.common.model.DFSFile;
import ai.sapper.cdc.core.model.DFSFileState;
import ai.sapper.cdc.core.model.EFileType;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.parquet.Strings;

public class ProtoBufUtils {
    public static DFSFile build(@NonNull DFSFileState fileState) throws Exception {
        DFSFile.Builder builder = DFSFile.newBuilder();
        builder.setInodeId(fileState.getId())
                .setPath(fileState.getHdfsFilePath());
        if (fileState.getFileType() != null
                && fileState.getFileType() != EFileType.UNKNOWN) {
            builder.setFileType(fileState.getFileType().name());
        }
        if (!Strings.isNullOrEmpty(fileState.getSchemaLocation())) {
            builder.setSchemaLocation(fileState.getSchemaLocation());
        }
        return builder.build();
    }

    public static boolean update(@NonNull DFSFileState fileState, @NonNull DFSFile file) throws Exception {
        Preconditions.checkArgument(fileState.getId() == file.getInodeId());
        boolean updated = false;
        if (file.hasFileType()) {
            EFileType fileType = EFileType.parse(file.getFileType());
            if (fileType != null && fileType != EFileType.UNKNOWN) {
                fileState.setFileType(fileType);
                updated = true;
            }
        }
        if (file.hasSchemaLocation()) {
            fileState.setSchemaLocation(file.getSchemaLocation());
            updated = true;
        }
        return updated;
    }

    public static boolean update(@NonNull DFSFileReplicaState replicaState, @NonNull DFSFile file) throws Exception {
        boolean updated = false;
        Preconditions.checkArgument(replicaState.getInode() == file.getInodeId());
        if (file.hasFileType()) {
            EFileType fileType = EFileType.parse(file.getFileType());
            if (fileType != null && fileType != EFileType.UNKNOWN) {
                replicaState.setFileType(fileType);
                updated = true;
            }
        }
        if (file.hasSchemaLocation()) {
            replicaState.setSchemaLocation(file.getSchemaLocation());
            updated = true;
        }
        return updated;
    }

    public static boolean update(@NonNull DFSFileState fileState, @NonNull DFSFileReplicaState replicaState) {
        boolean updated = false;
        if (fileState.getFileType() != null
                && fileState.getFileType() != EFileType.UNKNOWN) {
            replicaState.setFileType(fileState.getFileType());
            updated = true;
        }
        if (!Strings.isNullOrEmpty(fileState.getSchemaLocation())) {
            replicaState.setSchemaLocation(fileState.getSchemaLocation());
            updated = true;
        }
        return updated;
    }
}
