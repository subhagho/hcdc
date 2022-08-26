package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.model.*;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.core.io.*;
import ai.sapper.cdc.core.model.*;
import ai.sapper.hcdc.agents.common.*;
import ai.sapper.hcdc.agents.model.DFSBlockReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.cdc.common.model.services.SnapshotDoneRequest;
import ai.sapper.cdc.core.WebServiceClient;
import ai.sapper.cdc.core.connections.HdfsConnection;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.schema.SchemaManager;
import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Strings;
import jakarta.ws.rs.core.MediaType;
import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.hadoop.hdfs.HDFSBlockReader;

import java.io.IOException;
import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

public class EntityChangeTransactionReader extends TransactionProcessor {
    public static final String SERVICE_SNAPSHOT_DONE = "snapshotDone";
    private MessageSender<String, DFSChangeDelta> sender;
    private HCDCFileSystem fs;
    private HdfsConnection connection;
    private WebServiceClient client;
    private Archiver archiver;

    public EntityChangeTransactionReader withHdfsConnection(@NonNull HdfsConnection connection) {
        this.connection = connection;
        return this;
    }

    public EntityChangeTransactionReader withSenderQueue(@NonNull MessageSender<String, DFSChangeDelta> sender) {
        this.sender = sender;
        return this;
    }

    public EntityChangeTransactionReader withFileSystem(@NonNull HCDCFileSystem fs) {
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
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getDomain());
        schemaEntity.setEntity(message.value().getEntityName());
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
        stateManager().replicationLock().lock();
        try {
            DFSFileReplicaState rState = stateManager()
                    .replicaStateHelper()
                    .create(fileState.getId(),
                            fileState.getHdfsFilePath(),
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
            stateManager().replicationLock().unlock();
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

        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getDomain());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getHdfsFilePath()));
            }
        }
        if (!fileState.hasError() && rState.isEnabled()) {
            FSFile file = fs.get(fileState, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            rState = stateManager()
                    .replicaStateHelper()
                    .get(fileState.getId());
            rState.setState(EFileState.Updating);
            rState.setLastReplicatedTx(txId);
            rState.setLastReplicationTime(System.currentTimeMillis());
            rState = stateManager().replicaStateHelper().update(rState);
            LOGGER.debug(getClass(), txId, String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
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

        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getDomain());
        schemaEntity.setEntity(message.value().getEntityName());


        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getHdfsFilePath()));
            }
        }
        if (!fileState.hasError() && rState.isEnabled()) {
            FSFile file = fs.get(fileState, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            CDCDataConverter converter = new CDCDataConverter()
                    .withFileSystem(fs)
                    .withSchemaManager(NameNodeEnv.get().schemaManager());
            PathInfo outPath = converter.convert(fileState,
                    rState,
                    AvroChangeType.EChangeType.RecordDelete,
                    0,
                    txId);
            if (outPath == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("Failed to generate transaction delta. [path=%s]",
                                data.getFile().getPath()));
            }
            rState.addDelta(txId, outPath.pathConfig());
            DFSChangeData delta = DFSChangeData.newBuilder()
                    .setTransaction(data.getTransaction())
                    .setFile(data.getFile())
                    .setDomain(schemaEntity.getDomain())
                    .setEntityName(schemaEntity.getEntity())
                    .setFileSystem(fs.fileSystemCode())
                    .setOutputPath(outPath.path())
                    .build();
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(
                    message.value().getNamespace(),
                    delta,
                    DFSChangeData.class,
                    schemaEntity.getDomain(),
                    schemaEntity.getEntity(),
                    message.mode());
            sender.send(m);

            if (archiver != null) {
                String path = String.format("%s/%d", fileState.getHdfsFilePath(), fileState.getId());
                PathInfo tp = archiver.getTargetPath(path, schemaEntity);
                PathInfo ap = archiver.archive(file, tp, fs);
                rState.setStoragePath(ap.pathConfig());
            }
            file.delete();
            stateManager().replicationLock().lock();
            try {
                rState = stateManager()
                        .replicaStateHelper()
                        .get(fileState.getId());
                rState.setEnabled(false);
                rState.setLastReplicatedTx(txId);
                rState.setLastReplicationTime(System.currentTimeMillis());

                stateManager()
                        .replicaStateHelper()
                        .update(rState);
            } finally {
                stateManager().replicationLock().unlock();
            }
            LOGGER.debug(getClass(), txId,
                    String.format("Deleted file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
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
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getDomain());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getHdfsFilePath()));
            }
        }
        if (!fileState.hasError() && rState.canUpdate()) {
            FSFile file = fs.get(fileState, schemaEntity);
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
                    String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
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
                    fileState.getHdfsFilePath(),
                    String.format("File State out of sync, no block to update. [path=%s]",
                            fileState.getHdfsFilePath()));
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getDomain());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getHdfsFilePath()));
            }
        }
        if (!fileState.hasError() && rState.canUpdate()) {
            FSFile file = fs.get(fileState, schemaEntity);
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
                            fileState.getHdfsFilePath(),
                            String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()));
                } else if (bs.getDataSize() != block.getSize()) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()));
                }
                if (bs.blockIsFull()) continue;
                FSBlock bb = file.get(bs.getBlockId());
                if (bb == null) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()));
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
                    String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
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
        if (fileState.getId() != file.getInodeId()) {
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

        if (message.mode() == MessageObject.MessageMode.Snapshot) {
            SchemaEntity schemaEntity = new SchemaEntity();
            schemaEntity.setDomain(message.value().getDomain());
            schemaEntity.setEntity(message.value().getEntityName());
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
                .get(fileState.getId());
        if (rState == null) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        } else if (rState.getLastReplicatedTx() >= txId) {
            if (message.mode() == MessageObject.MessageMode.New) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message detected: [path=%s]", fileState.getHdfsFilePath()));
            }
        }
        SchemaEntity schemaEntity = rState.getEntity();
        long startTxId = rState.getLastReplicatedTx();
        if (!fileState.hasError() && rState.canUpdate()) {
            try (HDFSBlockReader reader = new HDFSBlockReader(connection.dfsClient(), rState.getHdfsPath())) {
                reader.init();
                FSFile file = fs.get(fileState, schemaEntity);
                if (file == null) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            data.getFile().getPath(),
                            String.format("FileSystem file not found. [path=%s]",
                                    data.getFile().getPath()));
                }
                SchemaManager schemaManager = NameNodeEnv.get().schemaManager();
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
                    PathInfo outPath = converter.convert(fileState,
                            rState,
                            AvroChangeType.EChangeType.RecordInsert,
                            startTxId,
                            txId);
                    if (outPath == null) {
                        throw new InvalidTransactionError(txId,
                                DFSError.ErrorCode.SYNC_STOPPED,
                                data.getFile().getPath(),
                                String.format("Failed to generate transaction delta. [path=%s]",
                                        data.getFile().getPath()));
                    }
                    rState.addDelta(txId, outPath.pathConfig());
                    DFSChangeData delta = DFSChangeData.newBuilder()
                            .setTransaction(data.getTransaction())
                            .setFile(data.getFile())
                            .setDomain(schemaEntity.getDomain())
                            .setEntityName(schemaEntity.getEntity())
                            .setFileSystem(fs.fileSystemCode())
                            .setOutputPath(outPath.path())
                            .build();

                    EntityDef schema = schemaManager.get(rState.getEntity());
                    if (schema != null) {
                        if (!Strings.isNullOrEmpty(schema.schemaPath())) {
                            rState.setSchemaLocation(schema.schemaPath());
                        }
                        if (schema.version() != null) {
                            if (prevSchema != null) {
                                compareSchemaVersions(prevSchema.version(),
                                        schema.version(),
                                        rState, data.getTransaction(),
                                        message, txId);
                            }
                            rState.setSchemaVersion(schema.version());
                        }
                    }

                    MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(
                            message.value().getNamespace(),
                            delta,
                            DFSChangeData.class,
                            schemaEntity.getDomain(),
                            schemaEntity.getEntity(),
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
                                fileState.getHdfsFilePath(),
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
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (!rState.canUpdate()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    private void compareSchemaVersions(SchemaVersion current,
                                       SchemaVersion updated,
                                       DFSFileReplicaState replicaState,
                                       DFSTransaction tnx,
                                       MessageObject<String, DFSChangeDelta> message,
                                       long txId) throws Exception {
        if (current.equals(updated) || current.compare(updated) <= 0) return;
        MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe
                .createSchemaChange(message.value().getNamespace(),
                        tnx, current, updated, replicaState, MessageObject.MessageMode.Schema
                );
        m.correlationId(message.id());

        sender.send(m);
    }

    private DFSFileReplicaState snapshotDone(DFSFileState fileState,
                                             DFSFileReplicaState replicaState) throws Exception {
        SnapshotDoneRequest request
                = new SnapshotDoneRequest(
                replicaState.getEntity(),
                replicaState.getSnapshotTxId(),
                fileState.getHdfsFilePath());
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
                    fileState.getHdfsPath(),
                    String.format("Error reading block from HDFS. [path=%s][block ID=%d]",
                            fileState.getHdfsPath(), source.getBlockId()));
        }
        if (detect
                && (fileState.getFileType() == null || fileState.getFileType() == EFileType.UNKNOWN)) {
            EFileType fileType = converter.detect(fileState.getHdfsPath(),
                    data.data().array(),
                    (int) data.dataSize());
            if (fileType != null && fileType != EFileType.UNKNOWN) {
                fileState.setFileType(fileType);
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
                DFSFileReplicaState rState = stateManager()
                        .replicaStateHelper()
                        .get(fileState.getId());
                if (rState != null) {
                    rState.setState(EFileState.Error);
                    if (tnx != null)
                        rState.setLastReplicatedTx(tnx.getTransactionId());
                    rState.setLastReplicationTime(System.currentTimeMillis());

                    stateManager().replicaStateHelper().update(rState);
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
                DFSFileReplicaState rState = stateManager()
                        .replicaStateHelper()
                        .get(fileState.getId());
                if (rState != null) {
                    rState.setState(EFileState.Error);
                    if (tnx != null)
                        rState.setLastReplicatedTx(tnx.getTransactionId());
                    rState.setLastReplicationTime(System.currentTimeMillis());

                    stateManager().replicaStateHelper().update(rState);
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
