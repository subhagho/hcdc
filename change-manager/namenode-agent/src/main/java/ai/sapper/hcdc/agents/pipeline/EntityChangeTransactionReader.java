package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.model.services.SnapshotDoneRequest;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.core.WebServiceClient;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.EncryptionHandler;
import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.cdc.core.utils.HFSHelper;
import ai.sapper.cdc.core.utils.SchemaEntityHelper;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.schema.SchemaManager;
import ai.sapper.hcdc.agents.common.*;
import ai.sapper.hcdc.agents.model.*;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.io.FSBlock;
import ai.sapper.hcdc.io.FSFile;
import ai.sapper.hcdc.io.HCDCFsUtils;
import ai.sapper.hcdc.messaging.HCDCChangeDeltaSerDe;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.hadoop.hdfs.HDFSBlockReader;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

public class EntityChangeTransactionReader extends TransactionProcessor {
    public static final String SERVICE_SNAPSHOT_DONE = "snapshotDone";
    private MessageSender<String, DFSChangeDelta> sender;
    private CDCFileSystem fs;
    private HdfsConnection connection;
    private WebServiceClient client;
    private Archiver archiver;
    private EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler;

    public EntityChangeTransactionReader(@NonNull String name) {
        super(name);
    }

    public EntityChangeTransactionReader withHdfsConnection(@NonNull HdfsConnection connection) {
        this.connection = connection;
        return this;
    }

    public EntityChangeTransactionReader withSenderQueue(@NonNull MessageSender<String, DFSChangeDelta> sender) {
        this.sender = sender;
        return this;
    }

    public EntityChangeTransactionReader withFileSystem(@NonNull CDCFileSystem fs) {
        this.fs = fs;
        return this;
    }

    public EntityChangeTransactionReader withClient(@NonNull WebServiceClient client) {
        this.client = client;
        return this;
    }

    public EntityChangeTransactionReader withArchiver(Archiver archiver) {
        this.archiver = archiver;
        return this;
    }

    public EntityChangeTransactionReader withEncryptionHandler(@NonNull EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler) {
        this.encryptionHandler = encryptionHandler;
        return this;
    }

    private void sendIgnoreTx(MessageObject<String, DFSChangeDelta> message, Object data) throws Exception {
        // Do nothing...
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAddFileTxMessage(@NonNull DFSFileAdd data,
                                        @NonNull MessageObject<String, DFSChangeDelta> message,
                                        @NonNull BaseTxId txId,
                                        boolean retry) throws Exception {
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());
        if (Strings.isNullOrEmpty(schemaEntity.getDomain()) ||
                Strings.isNullOrEmpty(schemaEntity.getEntity())) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getEntity().getEntity(),
                    new Exception(String.format("Invalid Schema Entity: domain or entity is NULL. [path=%s]",
                            data.getFile().getEntity().getEntity())))
                    .withFile(data.getFile());
        }
        registerFile(data.getFile(), schemaEntity, message, txId, retry);
    }

    private DFSFileReplicaState registerFile(DFSFile file,
                                             SchemaEntity schemaEntity,
                                             MessageObject<String, DFSChangeDelta> message,
                                             BaseTxId txId,
                                             boolean retry) throws Exception {
        String hdfsPath = file.getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(hdfsPath);
        if (fileState == null) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    hdfsPath,
                    new IOException(String.format("File not found. [path=%s]", hdfsPath)))
                    .withFile(file);
        }

        checkStaleInode(message, fileState, file);

        FSFile fsf = FileSystemHelper.createFile(fileState, fs, schemaEntity);
        try {
            DFSFileReplicaState rState = null;
            if (retry) {
                rState = stateManager()
                        .replicaStateHelper()
                        .get(schemaEntity,
                                fileState.getFileInfo().getInodeId());
            }
            if (rState == null)
                rState = stateManager()
                        .replicaStateHelper()
                        .create(fileState.getFileInfo(),
                                schemaEntity,
                                true);
            rState.setSnapshotTxId(fileState.getLastTnxId());
            rState.setSnapshotTime(System.currentTimeMillis());
            rState.setSnapshotReady(message.mode() != MessageObject.MessageMode.Snapshot);
            rState.setState(EFileState.New);
            rState.copyBlocks(fileState);
            rState.setStoragePath(fsf.directory().pathConfig());
            rState.setLastReplicatedTx(txId.getId());
            rState.setLastReplicationTime(System.currentTimeMillis());

            rState = stateManager().replicaStateHelper().update(rState);
            return rState;
        } catch (Exception ex) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    hdfsPath, ex);
        }
    }

    private DFSFileReplicaState getReplicationState(DFSFileState fileState,
                                                    SchemaEntity schemaEntity,
                                                    DFSFile file,
                                                    MessageObject<String, DFSChangeDelta> message,
                                                    long txId,
                                                    boolean retry) throws Exception {
        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    file.getEntity().getEntity(),
                    new Exception(String.format("File not setup for replication. [path=%s]",
                            file.getEntity().getEntity())));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New && !retry) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]",
                                fileState.getFileInfo().getHdfsPath()));
            } else if (retry && txId != rState.getLastReplicatedTx()) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(
                                String.format("Invalid processed transaction. [path=%s][expected tx=%d][replicated tx=%d]",
                                        fileState.getFileInfo().getHdfsPath(), txId, rState.getLastReplicatedTx())))
                        .withFile(file);
            }
        }
        return rState;
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAppendFileTxMessage(@NonNull DFSFileAppend data,
                                           @NonNull MessageObject<String, DFSChangeDelta> message,
                                           @NonNull BaseTxId txId,
                                           boolean retry) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    path,
                    new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            path)));
        }

        checkStaleInode(message, fileState, data.getFile());

        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());

        DFSFileReplicaState rState = getReplicationState(fileState,
                schemaEntity,
                data.getFile(),
                message,
                txId.getId(),
                retry);
        if (!fileState.hasError() && rState.isEnabled()) {
            FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);

            rState = stateManager()
                    .replicaStateHelper()
                    .get(schemaEntity, fileState.getFileInfo().getInodeId());
            rState.setState(EFileState.Updating);
            rState.setLastReplicatedTx(txId.getId());
            rState.setLastReplicationTime(System.currentTimeMillis());
            rState = stateManager().replicaStateHelper().update(rState);

            LOGGER.debug(getClass(), txId.getId(), String.format("Updating file. [path=%s]",
                    fileState.getFileInfo().getHdfsPath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(String.format("FileSystem sync error. [path=%s]",
                            fileState.getFileInfo().getHdfsPath())));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processDeleteFileTxMessage(@NonNull DFSFileDelete data,
                                           @NonNull MessageObject<String, DFSChangeDelta> message,
                                           @NonNull BaseTxId txId,
                                           boolean retry) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState == null) {
            if (!retry)
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        path,
                        new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                                path)))
                        .withFile(data.getFile());
            else
                return;
        }
        checkStaleInode(message, fileState, data.getFile());

        if (!fileState.checkDeleted()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    path,
                    new Exception(String.format("File not marked for delete. [path=%s]",
                            path)))
                    .withFile(data.getFile());
        }

        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState == null) {
            if (!retry)
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        path,
                        new Exception(String.format("File not setup for replication. [path=%s]",
                                path)))
                        .withFile(data.getFile());
            return;
        } else if (rState.getLastReplicatedTx() >= txId.getId()) {
            if (message.mode() == MessageObject.MessageMode.New && !retry) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]",
                                fileState.getFileInfo().getHdfsPath()));
            } else if (retry && txId.getId() != rState.getLastReplicatedTx()) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(
                                String.format("Invalid processed transaction. [path=%s][expected tx=%d][replicated tx=%d]",
                                        fileState.getFileInfo().getHdfsPath(), txId.getId(), rState.getLastReplicatedTx())))
                        .withFile(data.getFile());
            }
        }
        if (!fileState.hasError() && rState.isEnabled()) {
            FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);
            CDCDataConverter converter = new CDCDataConverter(NameNodeEnv.get(name()).dbSource())
                    .withFileSystem(fs)
                    .withSchemaManager(NameNodeEnv.get(name()).schemaManager());
            if (encryptionHandler != null) {
                converter.withEncryptionHandler(encryptionHandler);
            }
            CDCDataConverter.ConversionResponse response = converter.convert(fileState,
                    rState,
                    AvroChangeType.EChangeType.RecordDelete,
                    0,
                    txId);
            if (response.path() == null) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        path,
                        new Exception(String.format("Failed to generate transaction delta. [path=%s]",
                                path)))
                        .withFile(data.getFile());
            }
            DFSReplicationDelta rDelta = new DFSReplicationDelta();
            rDelta.setOp(AvroChangeType.EChangeType.RecordDelete);
            rDelta.setTransactionId(txId.getId());
            rDelta.setInodeId(rState.getFileInfo().getInodeId());
            rDelta.setFsPath(response.path().pathConfig());
            rDelta.setRecordCount(response.recordCount());
            rState.addDelta(rDelta);

            SchemaManager schemaManager = NameNodeEnv.get(name()).schemaManager();
            DFSFile dfile = data.getFile();
            AvroEntitySchema schema = schemaManager.get(rState.getEntity());
            if (schema != null) {
                if (!Strings.isNullOrEmpty(schema.getZkPath())) {
                    rState.getFileInfo().setSchemaLocation(schema.getZkPath());
                }
                dfile = ProtoBufUtils.update(dfile, schema.getZkPath());
            } else {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        path,
                        new Exception(String.format("Entity Schema not found. [entity=%s]",
                                rState.getEntity().toString())))
                        .withFile(data.getFile());
            }
            DFSChangeData.Builder delta = DFSChangeData.newBuilder()
                    .setTransaction(data.getTransaction())
                    .setFile(dfile)
                    .setDomain(schemaEntity.getDomain())
                    .setEntityName(schemaEntity.getEntity())
                    .setFileSystem(HFSHelper.fileSystemCode(fs))
                    .putAllOutputPath(response.path().pathConfig());

            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(delta.build(),
                    DFSChangeData.class,
                    schemaEntity,
                    message.mode());
            sender.send(m);

            if (archiver != null) {
                String p = String.format("%s/%d", fileState.getFileInfo().getHdfsPath(),
                        fileState.getFileInfo().getInodeId());
                PathInfo tp = archiver.getTargetPath(p, schemaEntity);
                PathInfo ap = archiver.archive(file.directory(), tp, fs);
                rState.setStoragePath(ap.pathConfig());
            }
            file.delete();

            rState.setEnabled(false);
            rState.setLastReplicatedTx(txId.getId());
            rState.setLastReplicationTime(System.currentTimeMillis());

            stateManager()
                    .replicaStateHelper()
                    .update(rState);

            LOGGER.debug(getClass(), txId.getId(),
                    String.format("Deleted file. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(String.format("FileSystem sync error. [path=%s]",
                            fileState.getFileInfo().getHdfsPath())))
                    .withFile(data.getFile());
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAddBlockTxMessage(@NonNull DFSBlockAdd data,
                                         @NonNull MessageObject<String, DFSChangeDelta> message,
                                         @NonNull BaseTxId txId,
                                         boolean retry) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    path,
                    new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            path)))
                    .withFile(data.getFile());
        }

        checkStaleInode(message, fileState, data.getFile());
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());

        DFSFileReplicaState rState = getReplicationState(fileState,
                schemaEntity,
                data.getFile(),
                message,
                txId.getId(),
                retry);
        if (!fileState.hasError() && rState.canUpdate()) {
            FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);

            DFSBlock dataBlock = data.getLastBlock();
            DFSBlockState block = fileState.get(dataBlock.getBlockId());
            if (block == null) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        path,
                        new Exception(String.format("File State block not found. [path=%s][block ID=%d]",
                                path, dataBlock.getBlockId())))
                        .withFile(data.getFile());
            }
            FSBlock bb = file.get(block.getBlockId());
            if (bb == null) {
                file.add(block);
            }
            DFSBlockReplicaState b = rState.get(block.getBlockId());
            if (b == null) {
                b = new DFSBlockReplicaState();
                b.setState(EFileState.New);
                b.setBlockId(block.getBlockId());
                b.setPrevBlockId(block.getPrevBlockId());
                rState.add(b);
            }
            b.setStartOffset(0);
            b.setDataSize(block.getDataSize());
            b.setUpdateTime(System.currentTimeMillis());

            rState.setLastReplicatedTx(txId.getId());
            rState.setLastReplicationTime(System.currentTimeMillis());
            rState = stateManager().replicaStateHelper().update(rState);
            LOGGER.debug(getClass(), txId.getId(),
                    String.format("Updating file. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(String.format("FileSystem sync error. [path=%s]",
                            fileState.getFileInfo().getHdfsPath())))
                    .withFile(data.getFile());
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processUpdateBlocksTxMessage(@NonNull DFSBlockUpdate data,
                                             @NonNull MessageObject<String, DFSChangeDelta> message,
                                             @NonNull BaseTxId txId,
                                             boolean retry) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    path,
                    new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            path)))
                    .withFile(data.getFile());
        }

        checkStaleInode(message, fileState, data.getFile());
        List<DFSBlock> blocks = data.getBlocksList();
        if (blocks.isEmpty()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(String.format("File State out of sync, no block to update. [path=%s]",
                            fileState.getFileInfo().getHdfsPath())))
                    .withFile(data.getFile());
        }
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());

        DFSFileReplicaState rState = getReplicationState(fileState,
                schemaEntity,
                data.getFile(),
                message,
                txId.getId(),
                retry);
        if (!fileState.hasError() && rState.canUpdate()) {
            FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);

            for (DFSBlock block : blocks) {
                DFSBlockState bs = fileState.get(block.getBlockId());
                if (bs == null) {
                    throw new InvalidTransactionError(txId.getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(
                                    String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                            fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                            .withFile(data.getFile());
                } else if (bs.getDataSize() != block.getSize()) {
                    throw new InvalidTransactionError(txId.getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(
                                    String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                            fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                            .withFile(data.getFile());
                }
                if (bs.blockIsFull()) continue;
                FSBlock bb = file.get(bs.getBlockId());
                if (bb == null) {
                    throw new InvalidTransactionError(txId.getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(
                                    String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                            fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                            .withFile(data.getFile());
                }
                DFSBlockReplicaState b = rState.get(bb.blockId());
                if (b == null) {
                    b = new DFSBlockReplicaState();
                    rState.add(b);
                    b.setState(EFileState.New);
                    b.setBlockId(bs.getBlockId());
                    b.setPrevBlockId(bs.getPrevBlockId());
                }

                b.setStartOffset(0);
                b.setDataSize(bs.getDataSize());
                b.setUpdateTime(System.currentTimeMillis());

                rState.setLastReplicatedTx(txId.getId());
                rState.setLastReplicationTime(System.currentTimeMillis());

                rState = stateManager().replicaStateHelper().update(rState);
            }
            LOGGER.debug(getClass(), txId.getId(),
                    String.format("Updating file. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(String.format("FileSystem sync error. [path=%s]",
                            fileState.getFileInfo().getHdfsPath())))
                    .withFile(data.getFile());
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processTruncateBlockTxMessage(@NonNull DFSBlockTruncate data,
                                              @NonNull MessageObject<String, DFSChangeDelta> message,
                                              @NonNull BaseTxId txId,
                                              boolean retry) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    path,
                    new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            path)))
                    .withFile(data.getFile());
        }
        if (fileState.getLastTnxId() >= txId.getId()) {
            LOGGER.warn(getClass(), txId.getId(),
                    String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                            message.id(), message.mode().name()));
            return;
        }
        checkStaleInode(message, fileState, data.getFile());
    }

    private void checkStaleInode(MessageObject<String, DFSChangeDelta> message,
                                 DFSFileState fileState,
                                 DFSFile file) throws InvalidMessageError {
        if (fileState.getFileInfo().getInodeId() != file.getInodeId()) {
            throw new InvalidMessageError(message.id(),
                    String.format("Stale transaction: [path=%s]",
                            file.getEntity().getEntity()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processCloseFileTxMessage(@NonNull DFSFileClose data,
                                          @NonNull MessageObject<String, DFSChangeDelta> message,
                                          @NonNull BaseTxId txId,
                                          boolean retry) throws Exception {
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());
        String path = data.getFile().getEntity().getEntity();
        if (message.mode() == MessageObject.MessageMode.Snapshot) {
            if (Strings.isNullOrEmpty(schemaEntity.getDomain()) ||
                    Strings.isNullOrEmpty(schemaEntity.getEntity())) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        path,
                        new Exception(
                                String.format("Invalid Schema Entity: domain or entity is NULL. [path=%s]",
                                        path)))
                        .withFile(data.getFile());
            }
            registerFile(data.getFile(), schemaEntity, message, txId, retry);
        }
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    path,
                    new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            path)))
                    .withFile(data.getFile());
        }
        checkStaleInode(message, fileState, data.getFile());

        DFSFileReplicaState rState = getReplicationState(fileState,
                schemaEntity,
                data.getFile(),
                message,
                txId.getId(),
                retry);
        schemaEntity = rState.getEntity();
        long startTxId = rState.getLastReplicatedTx();
        if (!fileState.hasError() && rState.canUpdate()) {
            try (HDFSBlockReader reader = new HDFSBlockReader(connection.dfsClient(),
                    rState.getFileInfo().getHdfsPath())) {
                reader.init(encryptionHandler);
                FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);

                SchemaManager schemaManager = NameNodeEnv.get(name()).schemaManager();
                AvroEntitySchema prevSchema = schemaManager.get(rState.getEntity());
                CDCDataConverter converter = new CDCDataConverter(NameNodeEnv.get(name()).dbSource())
                        .withFileSystem(fs)
                        .withSchemaManager(schemaManager);
                if (encryptionHandler != null) {
                    converter.withEncryptionHandler(encryptionHandler);
                }
                List<DFSBlock> blocks = data.getBlocksList();
                if (!blocks.isEmpty()) {
                    for (DFSBlock block : blocks) {
                        DFSBlockReplicaState bs = rState.get(block.getBlockId());
                        if (bs == null || !bs.canUpdate()) {
                            throw new InvalidTransactionError(txId.getId(),
                                    DFSError.ErrorCode.SYNC_STOPPED,
                                    path,
                                    new Exception(
                                            String.format("Block not registered for update. [path=%s][block ID=%d]",
                                                    path, block.getBlockId())))
                                    .withFile(data.getFile());
                        }
                        FSBlock fsb = file.get(block.getBlockId());
                        if (fsb == null) {
                            throw new InvalidTransactionError(txId.getId(),
                                    DFSError.ErrorCode.SYNC_STOPPED,
                                    path,
                                    new Exception(
                                            String.format("Block not found in FileSystem. [path=%s][block ID=%d]",
                                                    path, block.getBlockId())))
                                    .withFile(data.getFile());
                        }

                        long size = copyBlock(txId.getId(),
                                block,
                                rState,
                                bs, fsb, reader, converter,
                                bs.getPrevBlockId() < 0);
                    }
                }
                try {
                    DFSReplicationDelta rDelta = rState.getDelta(txId.getId());
                    if (rDelta != null) {
                        if (message.mode() == MessageObject.MessageMode.New && !retry) {
                            throw new InvalidTransactionError(txId.getId(),
                                    DFSError.ErrorCode.SYNC_STOPPED,
                                    path,
                                    new Exception(String.format("Invalid transaction state: Delta already generated. [path=%s]",
                                            path)))
                                    .withFile(data.getFile());
                        } else {
                            rState.removeDelta(txId.getId());
                            PathInfo pi = fs.get(rDelta.getFsPath());
                            if (pi.exists()) {
                                fs.delete(pi);
                            }
                        }
                    }
                    CDCDataConverter.ConversionResponse response = converter.convert(fileState,
                            rState,
                            AvroChangeType.EChangeType.RecordInsert,
                            startTxId,
                            txId);
                    if (response == null) {
                        throw new InvalidTransactionError(txId.getId(),
                                DFSError.ErrorCode.SYNC_STOPPED,
                                path,
                                new Exception(String.format("Failed to generate transaction delta. [path=%s]",
                                        path)))
                                .withFile(data.getFile());
                    }
                    rDelta = new DFSReplicationDelta();
                    rDelta.setOp(AvroChangeType.EChangeType.RecordInsert);
                    rDelta.setTransactionId(txId.getId());
                    rDelta.setInodeId(rState.getFileInfo().getInodeId());
                    rDelta.setFsPath(response.path().pathConfig());
                    rDelta.setRecordCount(response.recordCount());
                    rState.addDelta(rDelta);
                    if (response.overwrite()) {
                        rState.setRecordCount(response.recordCount());
                    } else {
                        rState.setRecordCount(rState.getRecordCount() + response.recordCount());
                    }
                    DFSFile dfile = data.getFile();
                    AvroEntitySchema schema = schemaManager.get(rState.getEntity());
                    if (schema != null) {
                        if (!Strings.isNullOrEmpty(schema.getZkPath())) {
                            rState.getFileInfo().setSchemaLocation(schema.getZkPath());
                        }
                        if (schema.getVersion() != null) {
                            if (prevSchema != null) {
                                dfile = compareSchemaVersions(prevSchema.getVersion(),
                                        schema.getVersion(),
                                        rState, data.getTransaction(),
                                        message,
                                        dfile);
                            } else {
                                dfile = compareSchemaVersions(null,
                                        schema.getVersion(),
                                        rState, data.getTransaction(),
                                        message,
                                        dfile);
                            }
                            rState.getFileInfo().setSchemaVersion(schema.getVersion());
                        }
                    } else {
                        throw new InvalidTransactionError(txId.getId(),
                                DFSError.ErrorCode.SYNC_STOPPED,
                                path,
                                new Exception(String.format("Entity Schema not found. [entity=%s]",
                                        rState.getEntity().toString())))
                                .withFile(data.getFile());
                    }

                    DFSChangeData delta = DFSChangeData.newBuilder()
                            .setTransaction(data.getTransaction())
                            .setFile(dfile)
                            .setDomain(schemaEntity.getDomain())
                            .setEntityName(schemaEntity.getEntity())
                            .setFileSystem(HFSHelper.fileSystemCode(fs))
                            .putAllOutputPath(response.path().pathConfig())
                            .build();

                    MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(delta,
                            DFSChangeData.class,
                            schemaEntity,
                            message.mode());
                    sender.send(m);
                } catch (IOException ex) {
                    throw new InvalidTransactionError(txId.getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            path, ex)
                            .withFile(data.getFile());
                }
                if (message.mode() == MessageObject.MessageMode.Snapshot) {
                    DFSFileReplicaState nState = snapshotDone(fileState, rState);
                    if (!nState.isSnapshotReady()) {
                        throw new InvalidTransactionError(txId.getId(),
                                DFSError.ErrorCode.SYNC_STOPPED,
                                fileState.getFileInfo().getHdfsPath(),
                                new Exception(String.format("Error marking Snapshot Done. [TXID=%d]", txId.getId())))
                                .withFile(data.getFile());
                    }
                }

                rState.setSnapshotReady(true);
                rState.setSnapshotTime(System.currentTimeMillis());
                rState.setState(EFileState.Finalized);
                rState.setLastReplicatedTx(txId.getId());
                rState.setLastReplicationTime(System.currentTimeMillis());

                rState = stateManager().replicaStateHelper().update(rState);
            }
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(
                            String.format("FileSystem sync error. [path=%s]",
                                    fileState.getFileInfo().getHdfsPath())))
                    .withFile(data.getFile());
        } else if (!rState.canUpdate()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    path,
                    new Exception(String.format("File not setup for replication. [path=%s]",
                            path)))
                    .withFile(data.getFile());
        }
    }

    private DFSFile compareSchemaVersions(SchemaVersion current,
                                          @NonNull SchemaVersion updated,
                                          @NonNull DFSFileReplicaState replicaState,
                                          @NonNull DFSTransaction tnx,
                                          @NonNull MessageObject<String, DFSChangeDelta> message,
                                          @NonNull DFSFile file) throws Exception {

        AvroChangeType.EChangeType op = null;
        boolean changed = false;
        if (current == null) {
            changed = true;
            op = AvroChangeType.EChangeType.EntityCreate;
        } else if ((current.equals(updated) || current.compare(updated) <= 0)) {
            changed = false;
        } else {
            op = AvroChangeType.EChangeType.EntityUpdate;
            changed = true;
        }
        SchemaManager schemaManager = NameNodeEnv.get(name()).schemaManager();
        AvroEntitySchema ned = schemaManager.get(replicaState.getEntity(), updated);
        if (ned == null) {
            throw new Exception(
                    String.format("Entity Schema not found. [entity=%s][version=%s]",
                            replicaState.getEntity().toString(),
                            updated.toString()));
        }
        String updatedPath = ned.getZkPath();
        if (changed) {
            String currentPath = null;
            if (current != null) {
                AvroEntitySchema ed = schemaManager.get(replicaState.getEntity(), current);
                if (ed == null) {
                    throw new Exception(
                            String.format("Entity Schema not found. [entity=%s][version=%s]",
                                    replicaState.getEntity().toString(),
                                    current.toString()));
                }
                currentPath = ed.getZkPath();
            }

            MessageObject<String, DFSChangeDelta> m = HCDCChangeDeltaSerDe
                    .createSchemaChange(tnx,
                            currentPath,
                            updatedPath,
                            op,
                            replicaState,
                            message.mode()
                    );
            m.correlationId(message.id());

            sender.send(m);

        }

        return ProtoBufUtils.update(file, updatedPath);
    }

    private DFSFileReplicaState snapshotDone(DFSFileState fileState,
                                             DFSFileReplicaState replicaState) throws Exception {
        SnapshotDoneRequest request
                = new SnapshotDoneRequest(
                replicaState.getEntity(),
                replicaState.getSnapshotTxId(),
                fileState.getFileInfo().getHdfsPath());
        return client.post(SERVICE_SNAPSHOT_DONE,
                DFSFileReplicaState.class,
                request,
                null,
                MediaType.APPLICATION_JSON);
    }

    private long copyBlock(long txId,
                           DFSBlock source,
                           DFSFileReplicaState fileState,
                           DFSBlockReplicaState blockState,
                           FSBlock fsBlock,
                           HDFSBlockReader reader,
                           CDCDataConverter converter,
                           boolean detect) throws Exception {
        int length = (int) (source.getEndOffset() + 1);
        HDFSBlockData data = reader.read(source.getBlockId(),
                source.getGenerationStamp(),
                0L,
                length);

        if (data == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(String.format("Error reading block from HDFS. [path=%s][block ID=%d]",
                            fileState.getFileInfo().getHdfsPath(), source.getBlockId())));
        }
        if (detect
                && (fileState.getFileInfo().getFileType() == null
                || fileState.getFileInfo().getFileType() == EFileType.UNKNOWN)) {
            EFileType fileType = converter.detect(fileState.getFileInfo().getHdfsPath(),
                    data.data().array(),
                    (int) data.dataSize());
            if (fileType != null && fileType != EFileType.UNKNOWN) {
                fileState.getFileInfo().setFileType(fileType);
            }
        }
        fsBlock.write(data.data().array());
        fsBlock.close();

        blockState.setStoragePath(fsBlock.path().pathConfig());
        blockState.setState(EFileState.Finalized);
        return data.dataSize();
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processRenameFileTxMessage(@NonNull DFSFileRename data,
                                           @NonNull MessageObject<String, DFSChangeDelta> message,
                                           @NonNull BaseTxId txId,
                                           boolean retry) throws Exception {
        throw new InvalidMessageError(message.id(), "Rename transaction should not come...");
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processIgnoreTxMessage(@NonNull DFSIgnoreTx data,
                                       @NonNull MessageObject<String, DFSChangeDelta> message,
                                       @NonNull BaseTxId txId) throws Exception {
        LOGGER.debug(getClass(), txId.getId(), String.format("Received Ignore Transaction: [ID=%d]", txId.getId()));
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processErrorTxMessage(@NonNull DFSError data,
                                      @NonNull MessageObject<String, DFSChangeDelta> message,
                                      @NonNull BaseTxId txId) throws Exception {
        DFSTransaction tnx = extractTransaction(data);
        if (data.hasFile()) {
            DFSFile df = data.getFile();
            DFSFileState fileState = stateManager()
                    .fileStateHelper()
                    .get(df.getEntity().getEntity());
            if (fileState != null) {
                SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
                if (schemaEntity != null) {
                    DFSFileReplicaState rState = stateManager()
                            .replicaStateHelper()
                            .get(schemaEntity, fileState.getFileInfo().getInodeId());
                    if (rState != null) {
                        rState.setState(EFileState.Error);
                        if (tnx != null)
                            rState.setLastReplicatedTx(tnx.getId());
                        rState.setLastReplicationTime(System.currentTimeMillis());

                        stateManager().replicaStateHelper().update(rState);
                    }
                }
            }
        }
        LOGGER.warn(getClass(), txId.getId(),
                String.format("Received Error Message: %s. [TX=%d][ERROR CODE=%s]",
                        data.getMessage(),
                        txId.getId(),
                        data.getCode().name()));
    }

    /**
     * @param message
     * @param data
     * @param te
     * @throws Exception
     */
    @Override
    public void handleError(@NonNull MessageObject<String, DFSChangeDelta> message,
                            @NonNull Object data,
                            @NonNull InvalidTransactionError te) throws Exception {
        DFSTransaction tnx = extractTransaction(data);
        if (!Strings.isNullOrEmpty(te.getHdfsPath())) {
            DFSFileState fileState = stateManager()
                    .fileStateHelper()
                    .get(te.getHdfsPath());
            if (fileState != null) {
                SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
                if (schemaEntity != null) {
                    DFSFileReplicaState rState = stateManager()
                            .replicaStateHelper()
                            .get(schemaEntity, fileState.getFileInfo().getInodeId());
                    if (rState != null) {
                        rState.setState(EFileState.Error);
                        if (tnx != null)
                            rState.setLastReplicatedTx(tnx.getId());
                        rState.setLastReplicationTime(System.currentTimeMillis());

                        stateManager().replicaStateHelper().update(rState);
                    }
                }
            }
        }
    }
}
