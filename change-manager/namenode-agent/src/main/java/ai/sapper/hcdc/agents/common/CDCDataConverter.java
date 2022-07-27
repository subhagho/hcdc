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
                            long startTxId,
                            long currentTxId) throws IOException {
        Preconditions.checkNotNull(fs);
        Preconditions.checkArgument(replicaState.getEntity() != null);
        Preconditions.checkArgument(replicaState.getStoragePath() != null);
        try {
            for (FormatConverter converter : CONVERTERS) {
                if (converter.canParse(fileState.getHdfsFilePath())) {
                    File output = convert(converter, fileState, replicaState, startTxId, currentTxId);
                    return upload(output, fileState, replicaState, currentTxId);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private PathInfo upload(File source,
                            DFSFileState fileState,
                            DFSFileReplicaState replicaState,
                            long txId) throws Exception {
        Preconditions.checkNotNull(fs);
        String dir = FilenameUtils.getPath(fileState.getHdfsFilePath());

        String uploadPath = String.format("%s/%d", dir, txId);
        PathInfo path = fs.get(uploadPath, replicaState.getEntity().getDomain());
        fs.mkdirs(path);

        return fs.upload(source, path);
    }

    private File convert(FormatConverter converter,
                         DFSFileState fileState,
                         DFSFileReplicaState replicaState,
                         long startTxId,
                         long currentTxId) throws Exception {
        File source = null;
        if (converter.supportsPartial()) {
            source = createDeltaFile(fileState, replicaState, startTxId, currentTxId);
        } else {
            source = createSourceFile(fileState, replicaState, currentTxId);
        }
        String fname = FilenameUtils.getName(replicaState.getHdfsPath());
        fname = FilenameUtils.removeExtension(fname);
        String path = String.format("%s/%s-%d.avro", fs.tempPath(), fname, currentTxId);
        return converter.convert(source, new File(path));
    }

    private File createSourceFile(DFSFileState fileState,
                                  DFSFileReplicaState replicaState,
                                  long currentTxId) throws Exception {
        PathInfo source = fs.get(replicaState.getStoragePath());
        if (!source.exists()) {
            throw new IOException(String.format("Change Set not found. [path=%s]", source.toString()));
        }
        String fname = FilenameUtils.getName(replicaState.getHdfsPath());
        String path = String.format("%s/%s", fs.tempPath(), fname);
        File file = new File(path);

        if (!ChangeSetHelper.createChangeSet(fs,
                fileState,
                replicaState,
                file,
                -1,
                currentTxId)) {
            return null;
        }
        return file;
    }

    private File createDeltaFile(DFSFileState fileState,
                                 DFSFileReplicaState replicaState,
                                 long startTxId,
                                 long currentTxId) throws Exception {
        PathInfo source = fs.get(replicaState.getStoragePath());
        if (!source.exists()) {
            throw new IOException(String.format("Change Set not found. [path=%s]", source.toString()));
        }
        String fname = FilenameUtils.getName(replicaState.getHdfsPath());
        String path = String.format("%s/%s", fs.tempPath(), fname);
        File file = new File(path);

        if (!ChangeSetHelper.createChangeSet(fs,
                fileState,
                replicaState,
                file,
                startTxId,
                currentTxId)) {
            return null;
        }
        return file;
    }
}
