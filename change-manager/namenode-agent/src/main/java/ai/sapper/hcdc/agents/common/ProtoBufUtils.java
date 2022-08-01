package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.common.model.DFSFile;
import ai.sapper.hcdc.common.utils.JSONUtils;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileType;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.parquet.Strings;

import java.util.Map;

public class ProtoBufUtils {
    public static DFSFile build(@NonNull DFSFileState fileState) throws Exception {
        DFSFile.Builder builder = DFSFile.newBuilder();
        builder.setInodeId(fileState.getId())
                .setPath(fileState.getHdfsFilePath());
        if (fileState.getFileType() != null
                && fileState.getFileType() != EFileType.UNKNOWN) {
            builder.setFileType(fileState.getFileType().name());
        }
        if (fileState.getSchemaLocation() != null
                && !fileState.getSchemaLocation().isEmpty()) {
            String json = JSONUtils.asString(fileState.getSchemaLocation(), Map.class);
            builder.setSchemaLocation(json);
        }
        return builder.build();
    }

    public static void update(@NonNull DFSFileState fileState, @NonNull DFSFile file) throws Exception {
        Preconditions.checkArgument(fileState.getId() == file.getInodeId());
        if (file.hasFileType()) {
            EFileType fileType = EFileType.parse(file.getFileType());
            if (fileType != null && fileType != EFileType.UNKNOWN) {
                fileState.setFileType(fileType);
            }
        }
        if (file.hasSchemaLocation()) {
            String json = file.getSchemaLocation();
            if (!Strings.isNullOrEmpty(json)) {
                Map<String, String> map = JSONUtils.read(json, Map.class);
                if (map != null && !map.isEmpty()) {
                    fileState.setSchemaLocation(map);
                }
            }
        }
    }

    public static void update(@NonNull DFSFileReplicaState replicaState, @NonNull DFSFile file) throws Exception {
        Preconditions.checkArgument(replicaState.getInode() == file.getInodeId());
        if (file.hasFileType()) {
            EFileType fileType = EFileType.parse(file.getFileType());
            if (fileType != null && fileType != EFileType.UNKNOWN) {
                replicaState.setFileType(fileType);
            }
        }
        if (file.hasSchemaLocation()) {
            String json = file.getSchemaLocation();
            if (!Strings.isNullOrEmpty(json)) {
                Map<String, String> map = JSONUtils.read(json, Map.class);
                if (map != null && !map.isEmpty()) {
                    replicaState.setSchemaLocation(map);
                }
            }
        }
    }

    public static void update(@NonNull DFSFileState fileState, @NonNull DFSFileReplicaState replicaState) {
        if (fileState.getFileType() != null
                && fileState.getFileType() != EFileType.UNKNOWN) {
            replicaState.setFileType(fileState.getFileType());
        }
        if (fileState.getSchemaLocation() != null
                && !fileState.getSchemaLocation().isEmpty()) {
            replicaState.setSchemaLocation(fileState.getSchemaLocation());
        }
    }
}
