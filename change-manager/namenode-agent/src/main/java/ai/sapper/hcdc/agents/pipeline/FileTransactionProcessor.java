package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.InvalidTransactionError;
import ai.sapper.hcdc.agents.common.TransactionProcessor;
import ai.sapper.hcdc.agents.namenode.HDFSSnapshotProcessor;
import ai.sapper.hcdc.agents.namenode.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.namenode.model.NameNodeTxState;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.core.io.FSBlock;
import ai.sapper.hcdc.core.io.FSFile;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.hcdc.core.messaging.InvalidMessageError;
import ai.sapper.hcdc.core.messaging.MessageObject;
import ai.sapper.hcdc.core.messaging.MessageSender;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileState;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.List;

public class FileTransactionProcessor extends TransactionProcessor {
    private MessageSender<String, DFSChangeDelta> sender;
    private FileSystem fs;

    public FileTransactionProcessor withSenderQueue(@NonNull MessageSender<String, DFSChangeDelta> sender) {
        this.sender = sender;
        return this;
    }

    public FileTransactionProcessor withFileSystem(@NonNull FileSystem fs) {
        this.fs = fs;
        return this;
    }

    /**
     * @param message
     * @return
     * @throws Exception
     */
    @Override
    public long checkMessageSequence(MessageObject<String, DFSChangeDelta> message) throws Exception {
        long txId = Long.parseLong(message.value().getTxId());
        if (message.mode() == MessageObject.MessageMode.New) {
            NameNodeTxState txState = stateManager().agentTxState();
            if (txId <= txState.getProcessedTxId()) {
                throw new InvalidMessageError(message.id(), String.format("Duplicate message: Transaction already processed. [TXID=%d][CURRENT=%d]", txId, txState.getProcessedTxId()));
            }
        }
        return txId;
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
    public void processAddFileTxMessage(DFSAddFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    message.value().getEntity(),
                    String.format("File not found. [path=%s]", message.value().getEntity()));
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (rState == null) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("File replication state not found. [path=%s]", fileState.getHdfsFilePath()));
        }
        FSFile file = FileSystemHelper.createFile(fileState, fs, schemaEntity);
        stateManager().replicationLock().lock();
        try {
            rState = stateManager().get(fileState.getId());
            rState.setStoragePath(file.directory().path());
            rState.setState(EFileState.New);

            rState = stateManager().update(rState);
        } finally {
            stateManager().replicationLock().unlock();
        }
        sender.send(message);
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAppendFileTxMessage(DFSAppendFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            stateManager().replicationLock().lock();
            try {
                rState = stateManager().get(fileState.getId());
                rState.setState(EFileState.Updating);

                rState = stateManager().update(rState);
            } finally {
                stateManager().replicationLock().unlock();
            }
            LOG.debug(String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processDeleteFileTxMessage(DFSDeleteFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());


        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            file.delete();
            stateManager().replicationLock().lock();
            try {
                rState = stateManager().get(fileState.getId());
                stateManager().delete(rState.getInode());
            } finally {
                stateManager().replicationLock().unlock();
            }
            LOG.debug(String.format("Deleted file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAddBlockTxMessage(DFSAddBlock data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.canUpdate()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            for (DFSBlockState block : fileState.getBlocks()) {
                FSBlock b = file.get(block.getBlockId());
                if (b == null) {
                    file.add(block);
                }
            }
            LOG.debug(String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null || !rState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processUpdateBlocksTxMessage(DFSUpdateBlocks data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        } else if (!fileState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, file not marked for update. [path=%s]",
                            data.getFile().getPath()));
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }
        List<DFSBlock> blocks = data.getBlocksList();
        if (blocks.isEmpty()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("File State out of sync, no block to update. [path=%s]",
                            fileState.getHdfsFilePath()));
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.canUpdate()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            for (DFSBlock block : blocks) {
                DFSBlockState bs = fileState.get(block.getBlockId());
                if (bs == null) {
                    throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()));
                } else if (bs.getDataSize() != block.getSize()) {
                    throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()));
                }
                if (bs.blockIsFull()) continue;
                FSBlock b = file.get(bs.getBlockId());
                if (b == null) {
                    throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()));
                }
            }
            LOG.debug(String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null || !rState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processTruncateBlockTxMessage(DFSTruncateBlock data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        } else if (!fileState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, file not marked for update. [path=%s]",
                            data.getFile().getPath()));
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processCloseFileTxMessage(DFSCloseFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        } else if (!fileState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, file not marked for update. [path=%s]",
                            data.getFile().getPath()));
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.canUpdate()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            List<DFSBlock> blocks = data.getBlocksList();
            if (!blocks.isEmpty()) {
                for (DFSBlock block : blocks) {
                    DFSBlockState bs = fileState.get(block.getBlockId());
                    if (bs == null) {
                        throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                                fileState.getHdfsFilePath(),
                                String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                        fileState.getHdfsFilePath(), block.getBlockId()));
                    } else if (bs.canUpdate()) {

                    } else if (bs.getDataSize() != block.getSize()) {
                        throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                                fileState.getHdfsFilePath(),
                                String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                        fileState.getHdfsFilePath(), block.getBlockId()));
                    } else {
                        throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                                fileState.getHdfsFilePath(),
                                String.format("File State out of sync, block state mismatch. [path=%s][blockID=%d]",
                                        fileState.getHdfsFilePath(), block.getBlockId()));
                    }
                }
            }
            stateManager().replicationLock().lock();
            try {
                rState = stateManager().get(fileState.getId());
                rState.setState(EFileState.Finalized);

                rState = stateManager().update(rState);
            } finally {
                stateManager().replicationLock().unlock();
            }
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null || !rState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    private long copyBlock() throws Exception {

        return -1;
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processRenameFileTxMessage(DFSRenameFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getSrcFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getSrcFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getSrcFile().getPath()));
        } else if (!fileState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getSrcFile().getPath(),
                    String.format("NameNode Replica out of sync, file not marked for update. [path=%s]",
                            data.getSrcFile().getPath()));
        }
        fileState = stateManager().markDeleted(data.getSrcFile().getPath());
        EFileState state = (fileState.getState() == EFileState.Error ? fileState.getState() : EFileState.New);
        DFSFileState nfs = stateManager().create(data.getDestFile().getPath(),
                data.getDestFile().getInodeId(),
                fileState.getCreatedTime(),
                fileState.getBlockSize(),
                state,
                txId);
        nfs.setBlocks(fileState.getBlocks());
        nfs.setNumBlocks(fileState.getNumBlocks());
        nfs.setDataSize(fileState.getDataSize());
        nfs.setUpdatedTime(data.getTransaction().getTimestamp());

        nfs = stateManager().update(nfs);

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (rState != null) {
            stateManager().delete(rState.getInode());
        }
        SchemaEntity schemaEntity = isRegistered(nfs.getHdfsFilePath());
        if (schemaEntity != null) {
            rState = stateManager().create(nfs.getId(), nfs.getHdfsFilePath(), schemaEntity, true);
            rState.setSnapshotTxId(nfs.getLastTnxId());
            rState.setSnapshotTime(System.currentTimeMillis());
            rState.setSnapshotReady(true);

            stateManager().update(rState);
            DFSAddFile addFile = HDFSSnapshotProcessor.generateSnapshot(nfs, true);
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    addFile,
                    DFSAddFile.class,
                    rState.getEntity().getDomain(),
                    rState.getEntity().getEntity(),
                    MessageObject.MessageMode.New);
            sender.send(m);
        } else if (nfs.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    nfs.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", nfs.getHdfsFilePath()));
        } else {
            sendIgnoreTx(message, data);
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processIgnoreTxMessage(DFSIgnoreTx data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        LOG.debug(String.format("Received Ignore Transaction: [ID=%d]", txId));
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processErrorTxMessage(DFSError data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        LOG.error(String.format("Received Error Message: %s. [TX=%d][ERROR CODE=%s]", data.getMessage(), txId, data.getCode().name()));
    }

    /**
     * @param message
     * @param data
     * @param te
     * @throws Exception
     */
    @Override
    public void processErrorMessage(MessageObject<String, DFSChangeDelta> message, Object data, InvalidTransactionError te) throws Exception {
        if (!Strings.isNullOrEmpty(te.getHdfsPath())) {
            DFSFileState fileState = stateManager().get(te.getHdfsPath());
            if (fileState != null) {
                stateManager().updateState(fileState.getHdfsFilePath(), EFileState.Error);
            }
        }
        DFSTransaction tnx = extractTransaction(data);
        if (tnx != null) {
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.createErrorTx(message.value().getNamespace(),
                    message.id(),
                    tnx,
                    te.getErrorCode(),
                    te.getMessage());
            sender.send(m);
        }
    }
}
