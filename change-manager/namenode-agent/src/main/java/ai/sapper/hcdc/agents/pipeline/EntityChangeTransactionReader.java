package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.model.AvroChangeType;
import ai.sapper.cdc.common.model.EntityDef;
import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.cdc.common.model.services.SnapshotDoneRequest;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.core.WebServiceClient;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.CDCFileSystem;
import ai.sapper.cdc.core.io.PathInfo;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.model.HDFSBlockData;
import ai.sapper.cdc.core.schema.SchemaManager;
import ai.sapper.hcdc.agents.common.CDCDataConverter;
import ai.sapper.hcdc.agents.common.InvalidTransactionError;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.TransactionProcessor;
import ai.sapper.hcdc.agents.model.*;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.common.utils.SchemaEntityHelper;
import ai.sapper.hcdc.io.FSBlock;
import ai.sapper.hcdc.io.FSFile;
import ai.sapper.hcdc.io.HCDCFsUtils;
import ai.sapper.hcdc.messaging.HCDCChangeDeltaSerDe;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.hadoop.hdfs.HDFSBlockReader;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

public class EntityChangeTransactionReader extends TransactionProcessor {
    public static final String SERVICE_SNAPSHOT_DONE = "snapshotDone";
    private MessageSender<String, DFSChangeDelta> sender;
    private CDCFileSystem fs;
    private HdfsConnection connection;
    private WebServiceClient client;
    private Archiver archiver;

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
    public void processAddFileTxMessage(DFSAddFile data,
                                        MessageObject<String, DFSChangeDelta> message,
                                        long txId) throws Exception {
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getSchema());
        if (Strings.isNullOrEmpty(schemaEntity.getDomain()) || Strings.isNullOrEmpty(schemaEntity.getEntity())) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("Invalid Schema Entity: domain or entity is NULL. [path=%s]",
                            data.getFile().getPath()));
        }
        registerFile(data.getFile(), schemaEntity, message, txId);
    }

    private DFSFileReplicaState registerFile(DFSFile file,
                                             SchemaEntity schemaEntity,
                                             MessageObject<String, DFSChangeDelta> message,
                                             long txId) throws Exception {
        String hdfsPath = file.getPath();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(hdfsPath);
        if (fileState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    hdfsPath,
                    String.format("File not found. [path=%s]", hdfsPath));
        }

        checkStaleInode(message, fileState, file);

        FSFile fsf = FileSystemHelper.createFile(fileState, fs, schemaEntity);
        stateManager().stateLock();
        try {
            DFSFileReplicaState rState = stateManager()
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
            rState.setLastReplicatedTx(txId);
            rState.setLastReplicationTime(System.currentTimeMillis());

            rState = stateManager().replicaStateHelper().update(rState);
            return rState;
        } catch (Exception ex) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    hdfsPath, ex.getLocalizedMessage());
        } finally {
            stateManager().stateUnlock();
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAppendFileTxMessage(DFSAppendFile data,
                                           MessageObject<String, DFSChangeDelta> message,
                                           long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }

        checkStaleInode(message, fileState, data.getFile());

        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getSchema());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]",
                                fileState.getFileInfo().getHdfsPath()));
            }
        }
        if (!fileState.hasError() && rState.isEnabled()) {
            FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);
            if (file == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            rState = stateManager()
                    .replicaStateHelper()
                    .get(schemaEntity, fileState.getFileInfo().getInodeId());
            rState.setState(EFileState.Updating);
            rState.setLastReplicatedTx(txId);
            rState.setLastReplicationTime(System.currentTimeMillis());
            rState = stateManager().replicaStateHelper().update(rState);
            LOGGER.debug(getClass(), txId, String.format("Updating file. [path=%s]",
                    fileState.getFileInfo().getHdfsPath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processDeleteFileTxMessage(DFSDeleteFile data,
                                           MessageObject<String, DFSChangeDelta> message,
                                           long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }
        checkStaleInode(message, fileState, data.getFile());

        if (!fileState.checkDeleted()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not marked for delete. [path=%s]",
                            data.getFile().getPath()));
        }

        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getSchema());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getFileInfo().getHdfsPath()));
            }
        }
        if (!fileState.hasError() && rState.isEnabled()) {
            FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);
            if (file == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            CDCDataConverter converter = new CDCDataConverter()
                    .withFileSystem(fs)
                    .withSchemaManager(NameNodeEnv.get(name()).schemaManager());
            CDCDataConverter.ConversionResponse response = converter.convert(fileState,
                    rState,
                    AvroChangeType.EChangeType.RecordDelete,
                    0,
                    txId);
            if (response.path() == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("Failed to generate transaction delta. [path=%s]",
                                data.getFile().getPath()));
            }
            DFSReplicationDelta rDelta = new DFSReplicationDelta();
            rDelta.setOp(AvroChangeType.EChangeType.RecordDelete);
            rDelta.setTransactionId(txId);
            rDelta.setInodeId(rState.getFileInfo().getInodeId());
            rDelta.setFsPath(response.path().pathConfig());
            rDelta.setRecordCount(response.recordCount());
            rState.addDelta(rDelta);

            DFSChangeData delta = DFSChangeData.newBuilder()
                    .setTransaction(data.getTransaction())
                    .setFile(data.getFile())
                    .setDomain(schemaEntity.getDomain())
                    .setEntityName(schemaEntity.getEntity())
                    .setFileSystem(fs.fileSystemCode())
                    .setOutputPath(JSONUtils.asString(response.path().pathConfig(), Map.class))
                    .build();
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(
                    message.value().getNamespace(),
                    delta,
                    DFSChangeData.class,
                    schemaEntity,
                    message.value().getSequence(),
                    message.mode());
            sender.send(m);

            if (archiver != null) {
                String path = String.format("%s/%d", fileState.getFileInfo().getHdfsPath(),
                        fileState.getFileInfo().getInodeId());
                PathInfo tp = archiver.getTargetPath(path, schemaEntity);
                PathInfo ap = archiver.archive(file.directory(), tp, fs);
                rState.setStoragePath(ap.pathConfig());
            }
            file.delete();
            stateManager().stateLock();
            try {
                rState = stateManager()
                        .replicaStateHelper()
                        .get(schemaEntity, fileState.getFileInfo().getInodeId());
                rState.setEnabled(false);
                rState.setLastReplicatedTx(txId);
                rState.setLastReplicationTime(System.currentTimeMillis());

                stateManager()
                        .replicaStateHelper()
                        .update(rState);
            } finally {
                stateManager().stateUnlock();
            }
            LOGGER.debug(getClass(), txId,
                    String.format("Deleted file. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAddBlockTxMessage(DFSAddBlock data,
                                         MessageObject<String, DFSChangeDelta> message,
                                         long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }

        checkStaleInode(message, fileState, data.getFile());
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getSchema());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getFileInfo().getHdfsPath()));
            }
        }
        if (!fileState.hasError() && rState.canUpdate()) {
            FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);
            if (file == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            DFSBlock dataBlock = data.getLastBlock();
            DFSBlockState block = fileState.get(dataBlock.getBlockId());
            if (block == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("File State block not found. [path=%s][block ID=%d]",
                                data.getFile().getPath(), dataBlock.getBlockId()));
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

            rState.setLastReplicatedTx(txId);
            rState.setLastReplicationTime(System.currentTimeMillis());
            rState = stateManager().replicaStateHelper().update(rState);
            LOGGER.debug(getClass(), txId,
                    String.format("Updating file. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processUpdateBlocksTxMessage(DFSUpdateBlocks data,
                                             MessageObject<String, DFSChangeDelta> message,
                                             long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }

        checkStaleInode(message, fileState, data.getFile());
        List<DFSBlock> blocks = data.getBlocksList();
        if (blocks.isEmpty()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    String.format("File State out of sync, no block to update. [path=%s]",
                            fileState.getFileInfo().getHdfsPath()));
        }
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getSchema());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getFileInfo().getHdfsPath()));
            }
        }
        if (!fileState.hasError() && rState.canUpdate()) {
            FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);
            if (file == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            for (DFSBlock block : blocks) {
                DFSBlockState bs = fileState.get(block.getBlockId());
                if (bs == null) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId()));
                } else if (bs.getDataSize() != block.getSize()) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId()));
                }
                if (bs.blockIsFull()) continue;
                FSBlock bb = file.get(bs.getBlockId());
                if (bb == null) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId()));
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

                rState.setLastReplicatedTx(txId);
                rState.setLastReplicationTime(System.currentTimeMillis());

                rState = stateManager().replicaStateHelper().update(rState);
            }
            LOGGER.debug(getClass(), txId,
                    String.format("Updating file. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processTruncateBlockTxMessage(DFSTruncateBlock data,
                                              MessageObject<String, DFSChangeDelta> message,
                                              long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }
        if (fileState.getLastTnxId() >= txId) {
            LOGGER.warn(getClass(), txId,
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
            throw new InvalidMessageError(message.id(), String.format("Stale transaction: [path=%s]", file.getPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processCloseFileTxMessage(DFSCloseFile data,
                                          MessageObject<String, DFSChangeDelta> message,
                                          long txId) throws Exception {
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getSchema());
        if (message.mode() == MessageObject.MessageMode.Snapshot) {
            if (Strings.isNullOrEmpty(schemaEntity.getDomain()) || Strings.isNullOrEmpty(schemaEntity.getEntity())) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("Invalid Schema Entity: domain or entity is NULL. [path=%s]", data.getFile().getPath()));
            }
            registerFile(data.getFile(), schemaEntity, message, txId);
        }
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }
        checkStaleInode(message, fileState, data.getFile());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getFileInfo().getHdfsPath()));
            }
        }
        schemaEntity = rState.getEntity();
        long startTxId = rState.getLastReplicatedTx();
        if (!fileState.hasError() && rState.canUpdate()) {
            try (HDFSBlockReader reader = new HDFSBlockReader(connection.dfsClient(),
                    rState.getFileInfo().getHdfsPath())) {
                reader.init();
                FSFile file = HCDCFsUtils.get(fileState, schemaEntity, fs);
                if (file == null) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            data.getFile().getPath(),
                            String.format("FileSystem file not found. [path=%s]",
                                    data.getFile().getPath()));
                }
                SchemaManager schemaManager = NameNodeEnv.get(name()).schemaManager();
                EntityDef prevSchema = schemaManager.get(rState.getEntity());
                CDCDataConverter converter = new CDCDataConverter()
                        .withFileSystem(fs)
                        .withSchemaManager(schemaManager);
                List<DFSBlock> blocks = data.getBlocksList();
                if (!blocks.isEmpty()) {
                    for (DFSBlock block : blocks) {
                        DFSBlockReplicaState bs = rState.get(block.getBlockId());
                        if (bs == null || !bs.canUpdate()) {
                            throw new InvalidTransactionError(txId,
                                    DFSError.ErrorCode.SYNC_STOPPED,
                                    data.getFile().getPath(),
                                    String.format("Block not registered for update. [path=%s][block ID=%d]",
                                            data.getFile().getPath(), block.getBlockId()));
                        }
                        FSBlock fsb = file.get(block.getBlockId());
                        if (fsb == null) {
                            throw new InvalidTransactionError(txId,
                                    DFSError.ErrorCode.SYNC_STOPPED,
                                    data.getFile().getPath(),
                                    String.format("Block not found in FileSystem. [path=%s][block ID=%d]",
                                            data.getFile().getPath(), block.getBlockId()));
                        }

                        long size = copyBlock(txId,
                                block,
                                rState,
                                bs, fsb, reader, converter,
                                bs.getPrevBlockId() < 0);
                    }
                }
                try {
                    CDCDataConverter.ConversionResponse response = converter.convert(fileState,
                            rState,
                            AvroChangeType.EChangeType.RecordInsert,
                            startTxId,
                            txId);
                    if (response == null) {
                        throw new InvalidTransactionError(txId,
                                DFSError.ErrorCode.SYNC_STOPPED,
                                data.getFile().getPath(),
                                String.format("Failed to generate transaction delta. [path=%s]",
                                        data.getFile().getPath()));
                    }
                    DFSReplicationDelta rDelta = new DFSReplicationDelta();
                    rDelta.setOp(AvroChangeType.EChangeType.RecordDelete);
                    rDelta.setTransactionId(txId);
                    rDelta.setInodeId(rState.getFileInfo().getInodeId());
                    rDelta.setFsPath(response.path().pathConfig());
                    rDelta.setRecordCount(response.recordCount());
                    rState.addDelta(rDelta);
                    if (response.overwrite()) {
                        rState.setRecordCount(response.recordCount());
                    } else {
                        rState.setRecordCount(rState.getRecordCount() + response.recordCount());
                    }
                    DFSChangeData delta = DFSChangeData.newBuilder()
                            .setTransaction(data.getTransaction())
                            .setFile(data.getFile())
                            .setDomain(schemaEntity.getDomain())
                            .setEntityName(schemaEntity.getEntity())
                            .setFileSystem(fs.fileSystemCode())
                            .setOutputPath(JSONUtils.asString(response.path().pathConfig(), Map.class))
                            .build();

                    EntityDef schema = schemaManager.get(rState.getEntity());
                    if (schema != null) {
                        if (!Strings.isNullOrEmpty(schema.schemaPath())) {
                            rState.getFileInfo().setSchemaLocation(schema.schemaPath());
                        }
                        if (schema.version() != null) {
                            if (prevSchema != null) {
                                compareSchemaVersions(prevSchema.version(),
                                        schema.version(),
                                        rState, data.getTransaction(),
                                        message, txId);
                            } else {

                            }
                            rState.getFileInfo().setSchemaVersion(schema.version());
                        }
                    }

                    MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(
                            message.value().getNamespace(),
                            delta,
                            DFSChangeData.class,
                            schemaEntity,
                            message.value().getSequence(),
                            message.mode());
                    sender.send(m);
                } catch (IOException ex) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            data.getFile().getPath(),
                            String.format("Error converting change delta to Avro. [path=%s]",
                                    data.getFile().getPath()));
                }
                if (message.mode() == MessageObject.MessageMode.Snapshot) {
                    DFSFileReplicaState nState = snapshotDone(fileState, rState);
                    if (!nState.isSnapshotReady()) {
                        throw new InvalidTransactionError(txId,
                                DFSError.ErrorCode.SYNC_STOPPED,
                                fileState.getFileInfo().getHdfsPath(),
                                String.format("Error marking Snapshot Done. [TXID=%d]", txId));
                    }
                }

                rState.setSnapshotReady(true);
                rState.setSnapshotTime(System.currentTimeMillis());
                rState.setState(EFileState.Finalized);
                rState.setLastReplicatedTx(txId);
                rState.setLastReplicationTime(System.currentTimeMillis());

                rState = stateManager().replicaStateHelper().update(rState);
            }
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getFileInfo().getHdfsPath()));
        } else if (!rState.canUpdate()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    private void compareSchemaVersions(SchemaVersion current,
                                       @NonNull SchemaVersion updated,
                                       @NonNull DFSFileReplicaState replicaState,
                                       @NonNull DFSTransaction tnx,
                                       @NonNull MessageObject<String, DFSChangeDelta> message,
                                       long txId) throws Exception {
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
        if (changed) {
            MessageObject<String, DFSChangeDelta> m = HCDCChangeDeltaSerDe
                    .createSchemaChange(tnx,
                            current,
                            updated, op,
                            replicaState,
                            MessageObject.MessageMode.Schema
                    );
            m.correlationId(message.id());

            sender.send(m);
        }
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
                    String.format("Error reading block from HDFS. [path=%s][block ID=%d]",
                            fileState.getFileInfo().getHdfsPath(), source.getBlockId()));
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
    public void processRenameFileTxMessage(DFSRenameFile data,
                                           MessageObject<String, DFSChangeDelta> message,
                                           long txId) throws Exception {
        throw new InvalidMessageError(message.id(), "Rename transaction should not come...");
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processIgnoreTxMessage(DFSIgnoreTx data,
                                       MessageObject<String, DFSChangeDelta> message,
                                       long txId) throws Exception {
        LOGGER.debug(getClass(), txId, String.format("Received Ignore Transaction: [ID=%d]", txId));
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processErrorTxMessage(DFSError data,
                                      MessageObject<String, DFSChangeDelta> message,
                                      long txId) throws Exception {
        DFSTransaction tnx = extractTransaction(data);
        if (data.hasFile()) {
            DFSFile df = data.getFile();
            DFSFileState fileState = stateManager()
                    .fileStateHelper()
                    .get(df.getPath());
            if (fileState != null) {
                SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
                if (schemaEntity != null) {
                    DFSFileReplicaState rState = stateManager()
                            .replicaStateHelper()
                            .get(schemaEntity, fileState.getFileInfo().getInodeId());
                    if (rState != null) {
                        rState.setState(EFileState.Error);
                        if (tnx != null)
                            rState.setLastReplicatedTx(tnx.getTransactionId());
                        rState.setLastReplicationTime(System.currentTimeMillis());

                        stateManager().replicaStateHelper().update(rState);
                    }
                }
            }
        }
        LOGGER.warn(getClass(), txId,
                String.format("Received Error Message: %s. [TX=%d][ERROR CODE=%s]", data.getMessage(), txId, data.getCode().name()));
    }

    /**
     * @param message
     * @param data
     * @param te
     * @throws Exception
     */
    @Override
    public void handleError(MessageObject<String, DFSChangeDelta> message,
                            Object data,
                            InvalidTransactionError te) throws Exception {
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
                            rState.setLastReplicatedTx(tnx.getTransactionId());
                        rState.setLastReplicationTime(System.currentTimeMillis());

                        stateManager().replicaStateHelper().update(rState);
                    }
                }
            }
        }
        if (tnx != null) {
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.createErrorTx(message.value().getNamespace(),
                    message.id(),
                    tnx,
                    te.getErrorCode(),
                    te.getMessage(),
                    te.getFile());
            sender.send(m);
        }
    }
}
