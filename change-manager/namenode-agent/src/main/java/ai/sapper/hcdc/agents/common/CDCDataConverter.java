package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.agents.common.converter.AvroConverter;
import ai.sapper.hcdc.agents.common.converter.ParquetConverter;
import ai.sapper.hcdc.agents.namenode.model.DFSFileReplicaState;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.model.DFSFileState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class CDCDataConverter {
    private static final FormatConverter[] CONVERTERS = {new ParquetConverter(), new AvroConverter()};

    private FileSystem fs;

    public CDCDataConverter withFileSystem(@NonNull FileSystem fs) {
        this.fs = fs;
        return this;
    }

    public PathInfo convert(@NonNull DFSFileState fileState,
                            @NonNull DFSFileReplicaState replicaState,
                            long txId) throws IOException {
        Preconditions.checkNotNull(fs);
        Preconditions.checkArgument(replicaState.getEntity() != null);
        Preconditions.checkArgument(replicaState.getStoragePath() != null);
        try {
            for (FormatConverter converter : CONVERTERS) {
                if (converter.canParse(fileState.getHdfsFilePath())) {
                    File output = convert(converter, replicaState, txId);
                    return upload(output, fileState, replicaState, txId);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private PathInfo upload(File source, DFSFileState fileState,
                            DFSFileReplicaState replicaState,
                            long txId) throws Exception {
        Preconditions.checkNotNull(fs);
        String dir = FilenameUtils.getPath(fileState.getHdfsFilePath());

        String uploadPath = String.format("%s/%d", dir, txId);
        PathInfo path = fs.get(uploadPath, replicaState.getEntity().getDomain());
        return fs.upload(source, path);
    }

    private File convert(FormatConverter converter, DFSFileReplicaState replicaState, long txId) throws Exception {
        File source = null;
        if (converter.supportsPartial()) {
            source = createDeltaFile(replicaState, txId);
        } else {
            source = createSourceFile(replicaState, txId);
        }
        String fname = FilenameUtils.getName(replicaState.getHdfsPath());
        String path = String.format("%s/%s-%d.avro", fs.tempPath(), fname, txId);
        return converter.convert(source, new File(path));
    }

    private File createSourceFile(DFSFileReplicaState replicaState, long txId) throws Exception {
        PathInfo source = fs.get(replicaState.getStoragePath());
        if (!source.exists()) {
            throw new IOException(String.format("Change Set not found. [path=%s]", source.toString()));
        }
        return null;
    }

    private File createDeltaFile(DFSFileReplicaState replicaState, long txId) throws Exception {
        return null;
    }
}
