package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.io.EncryptionHandler;
import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.model.DbSource;
import ai.sapper.cdc.entity.schema.SchemaManager;
import ai.sapper.hcdc.agents.common.converter.AvroConverter;
import ai.sapper.hcdc.agents.common.converter.ParquetConverter;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hdfs.HDFSBlockReader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

@Getter
@Accessors(fluent = true)
public class CDCDataConverter {
    private FormatConverter[] CONVERTERS;

    private CDCFileSystem fs;
    private HdfsConnection hdfsConnection;
    private EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler;

    public CDCDataConverter(@NonNull DbSource source) {
        CONVERTERS = new FormatConverter[2];
        CONVERTERS[0] = new AvroConverter(source);
        CONVERTERS[1] = new ParquetConverter(source);
    }

    public CDCDataConverter withFileSystem(@NonNull CDCFileSystem fs) {
        this.fs = fs;
        return this;
    }

    public CDCDataConverter withHdfsConnection(@NonNull HdfsConnection hdfsConnection) {
        this.hdfsConnection = hdfsConnection;
        return this;
    }

    public CDCDataConverter withSchemaManager(@NonNull SchemaManager schemaManager) {
        for (FormatConverter converter : CONVERTERS) {
            converter.withSchemaManager(schemaManager);
        }
        return this;
    }

    public CDCDataConverter withEncryptionHandler(@NonNull EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler) {
        this.encryptionHandler = encryptionHandler;
        return this;
    }

    public EFileType detect(@NonNull String path, byte[] data, int length) throws IOException {
        try {
            for (FormatConverter converter : CONVERTERS) {
                if (converter.detect(path, data, length)) return converter.fileType();
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public ConversionResponse convert(@NonNull DFSFileState fileState,
                                      @NonNull DFSFileReplicaState replicaState,
                                      @NonNull AvroChangeType.EChangeType op,
                                      long startTxId,
                                      @NonNull BaseTxId currentTxId) throws IOException {
        Preconditions.checkNotNull(fs);
        Preconditions.checkArgument(replicaState.getEntity() != null);
        Preconditions.checkArgument(replicaState.getStoragePath() != null);
        try {
            for (FormatConverter converter : CONVERTERS) {
                if (converter.canParse(fileState.getFileInfo().getHdfsPath(),
                        replicaState.getFileInfo().getFileType())) {
                    FormatConverter.Response response = convert(converter, fileState, replicaState, startTxId, currentTxId, op);
                    PathInfo uploaded = upload(response.file(), fileState, replicaState, currentTxId);
                    ConversionResponse cr = new ConversionResponse();
                    cr.path = uploaded;
                    cr.recordCount = response.recordCount();
                    cr.overwrite = !converter.supportsPartial();
                    return cr;
                }
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public ExtractSchemaResponse extractSchema(@NonNull DFSFileState fileState,
                                               @NonNull SchemaEntity schemaEntity) throws IOException {
        Preconditions.checkNotNull(hdfsConnection);
        try {
            try (HDFSBlockReader reader = new HDFSBlockReader(hdfsConnection.dfsClient(), fileState.getFileInfo().getHdfsPath())) {
                reader.init(encryptionHandler);
                DFSBlockState blockState = fileState.findFirstBlock();
                if (blockState == null) {
                    throw new Exception(
                            String.format("Error fetching first block from FileState. [path=%s]",
                                    fileState.getFileInfo().getHdfsPath()));
                }
                HDFSBlockData data = reader.read(blockState.getBlockId(),
                        blockState.getGenerationStamp(),
                        0L,
                        -1);
                if (data != null) {
                    for (FormatConverter converter : CONVERTERS) {
                        if (converter.detect(fileState.getFileInfo().getHdfsPath(),
                                data.data().array(),
                                (int) data.dataSize())) {
                            EFileType fileType = converter.fileType();
                            AvroEntitySchema schema = (AvroEntitySchema) converter.extractSchema(reader,
                                    fileState, schemaEntity);
                            if (schema != null) {
                                ExtractSchemaResponse response = new ExtractSchemaResponse();
                                return response.fileType(fileType)
                                        .schema(schema.getSchema())
                                        .version(schema.getVersion());
                            }
                        }
                    }
                }
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public FormatConverter getConverter(@NonNull EFileType fileType) {
        for (FormatConverter converter : CONVERTERS) {
            if (converter.fileType() == fileType) return converter;
        }
        return null;
    }

    private PathInfo upload(File source,
                            DFSFileState fileState,
                            DFSFileReplicaState replicaState,
                            BaseTxId txId) throws Exception {
        Preconditions.checkNotNull(fs);

        String uploadPath = String.format("%s/%s/%d/%d/%d",
                replicaState.getEntity().getDomain(),
                replicaState.getEntity().getEntity(),
                replicaState.getFileInfo().getInodeId(),
                txId.getId(),
                txId.getSequence());
        PathInfo path = fs.get(uploadPath, replicaState.getEntity().getDomain());
        fs.mkdirs(path);

        return fs.upload(source, path);
    }

    private FormatConverter.Response convert(FormatConverter converter,
                                             DFSFileState fileState,
                                             DFSFileReplicaState replicaState,
                                             long startTxId,
                                             BaseTxId currentTxId,
                                             @NonNull AvroChangeType.EChangeType op) throws Exception {
        File source = null;
        if (converter.supportsPartial()) {
            source = createDeltaFile(fileState, replicaState, startTxId, currentTxId);
        } else {
            source = createSourceFile(fileState, replicaState, currentTxId);
        }
        if (source == null) {
            throw new Exception(String.format("Empty file content. [entity=%s]", fileState.getFileInfo().getHdfsPath()));
        }
        String fname = FilenameUtils.getName(replicaState.getFileInfo().getHdfsPath());
        fname = FilenameUtils.removeExtension(fname);
        String path = PathUtils.formatPath(String.format("%s/%s-%d-%d.proto",
                fs.tempPath(),
                fname,
                currentTxId.getId(),
                currentTxId.getSequence()));
        return converter.convert(source,
                new File(path),
                fileState,
                replicaState.getEntity(),
                op,
                currentTxId,
                !replicaState.isSnapshotReady());

    }

    private File createSourceFile(DFSFileState fileState,
                                  DFSFileReplicaState replicaState,
                                  BaseTxId currentTxId) throws Exception {
        PathInfo source = fs.get(replicaState.getStoragePath());
        if (!source.exists()) {
            throw new IOException(String.format("Change Set not found. [path=%s]", source.toString()));
        }
        String fname = FilenameUtils.getName(replicaState.getFileInfo().getHdfsPath());
        String path = String.format("%s/%s", fs.tempPath(), fname);
        File file = new File(path);

        if (!ChangeSetHelper.createChangeSet(fs,
                fileState,
                replicaState,
                file,
                -1,
                currentTxId.getId())) {
            return null;
        }
        return file;
    }

    private File createDeltaFile(DFSFileState fileState,
                                 DFSFileReplicaState replicaState,
                                 long startTxId,
                                 BaseTxId currentTxId) throws Exception {
        PathInfo source = fs.get(replicaState.getStoragePath());
        if (!source.exists()) {
            throw new IOException(String.format("Change Set not found. [path=%s]", source.toString()));
        }
        String fname = FilenameUtils.getName(replicaState.getFileInfo().getHdfsPath());
        String path = String.format("%s/%s", fs.tempPath(), fname);
        File file = new File(path);

        if (!ChangeSetHelper.createChangeSet(fs,
                fileState,
                replicaState,
                file,
                startTxId,
                currentTxId.getId())) {
            return null;
        }
        return file;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ExtractSchemaResponse {
        private EFileType fileType;
        private Schema schema;
        private SchemaVersion version;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ConversionResponse {
        private PathInfo path;
        private long recordCount;
        private boolean overwrite = false;
    }
}
