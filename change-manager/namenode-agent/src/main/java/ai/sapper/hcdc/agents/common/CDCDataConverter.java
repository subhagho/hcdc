package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.agents.common.converter.AvroConverter;
import ai.sapper.hcdc.agents.common.converter.ParquetConverter;
import ai.sapper.hcdc.agents.namenode.model.DFSFileReplicaState;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.PathInfo;
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

    public PathInfo convert(@NonNull DFSFileReplicaState fileState, long txId) throws IOException {
        Preconditions.checkNotNull(fs);
        try {
            for (FormatConverter converter : CONVERTERS) {
                if (converter.canParse(fileState.getHdfsPath())) {
                    File output = convert(converter, fileState, txId);
                    return upload(output, fileState);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private PathInfo upload(File source, DFSFileReplicaState fileState) throws Exception {
        return null;
    }

    private File convert(FormatConverter converter, DFSFileReplicaState fileState, long txId) throws Exception {
        File source = null;
        if (converter.supportsPartial()) {
            source = createDeltaFile(fileState, txId);
        } else {
            source = createSourceFile(fileState, txId);
        }
        String fname = FilenameUtils.getName(fileState.getHdfsPath());
        String path = String.format("%s/%s-%d.avro", System.getProperty("java.io.tmpdir"), fname, txId);
        return converter.convert(source, new File(path));
    }

    private File createSourceFile(DFSFileReplicaState fileState, long txId) throws Exception {
        return null;
    }

    private File createDeltaFile(DFSFileReplicaState fileState, long txId) throws Exception {
        return null;
    }
}
