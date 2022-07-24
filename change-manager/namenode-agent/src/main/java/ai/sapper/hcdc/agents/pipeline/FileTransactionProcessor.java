package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.InvalidTransactionError;
import ai.sapper.hcdc.agents.common.TransactionProcessor;
import ai.sapper.hcdc.agents.namenode.HDFSSnapshotProcessor;
import ai.sapper.hcdc.agents.namenode.model.DFSReplicationState;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.hcdc.core.messaging.InvalidMessageError;
import ai.sapper.hcdc.core.messaging.MessageObject;
import ai.sapper.hcdc.core.messaging.MessageSender;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EBlockState;
import ai.sapper.hcdc.core.model.EFileState;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.List;

public class FileTransactionProcessor extends TransactionProcessor {
    private MessageSender<String, DFSChangeDelta> sender;

    public TransactionProcessor withSenderQueue(@NonNull MessageSender<String, DFSChangeDelta> sender) {
        this.sender = sender;
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
        return txId;
    }

    private void sendIgnoreTx(MessageObject<String, DFSChangeDelta> message, Object data) throws Exception {
        DFSTransaction tnx = extractTransaction(data);
        if (tnx != null) {
            MessageObject<String, DFSChangeDelta> im = ChangeDeltaSerDe.createIgnoreTx(message.value().getNamespace(),
                    tnx,
                    message.mode());
            sender.send(im);
        } else {
            throw new InvalidMessageError(message.id(), "Transaction data not found in message.");
        }
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
        if (fileState != null) {
            if (fileState.getLastTnxId() >= txId) {
                LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                        message.id(), message.mode().name()));
                return;
            } else if (!fileState.checkDeleted()) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getHdfsFilePath(),
                        String.format("Valid File already exists. [path=%s]", fileState.getHdfsFilePath()));
            }
        }
        fileState = stateManager().create(data.getFile().getPath(),
                data.getFile().getInodeId(),
                data.getModifiedTime(),
                data.getBlockSize(),
                EFileState.New,
                data.getTransaction().getTransactionId());
        List<DFSBlock> blocks = data.getBlocksList();
        if (!blocks.isEmpty()) {
            long prevBlockId = -1;
            for (DFSBlock block : blocks) {
                fileState = stateManager().addOrUpdateBlock(fileState.getHdfsFilePath(),
                        block.getBlockId(),
                        prevBlockId,
                        data.getModifiedTime(),
                        block.getSize(),
                        block.getGenerationStamp(),
                        EBlockState.New,
                        data.getTransaction().getTransactionId());
                prevBlockId = block.getBlockId();
            }
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        DFSReplicationState rState = stateManager().create(fileState.getId(), fileState.getHdfsFilePath(), schemaEntity, true);
        rState.setSnapshotTxId(fileState.getLastTnxId());
        rState.setSnapshotTime(System.currentTimeMillis());
        rState.setSnapshotReady(true);

        stateManager().update(rState);
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
        fileState = stateManager().updateState(fileState.getHdfsFilePath(), EFileState.Updating);

        DFSReplicationState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            sender.send(message);
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
        fileState = stateManager().markDeleted(fileState.getHdfsFilePath());

        DFSReplicationState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            stateManager().delete(fileState.getId());
            sender.send(message);
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
        long lastBlockId = -1;
        if (data.hasPenultimateBlock()) {
            lastBlockId = data.getPenultimateBlock().getBlockId();
        }
        fileState = stateManager().addOrUpdateBlock(fileState.getHdfsFilePath(),
                data.getLastBlock().getBlockId(),
                lastBlockId,
                data.getTransaction().getTimestamp(),
                data.getLastBlock().getSize(),
                data.getLastBlock().getGenerationStamp(),
                EBlockState.New,
                data.getTransaction().getTransactionId());

        DFSReplicationState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            sender.send(message);
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

            stateManager().updateState(fileState.getHdfsFilePath(), bs.getBlockId(), EBlockState.Updating);
        }
        DFSReplicationState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            sender.send(message);
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
                    fileState = stateManager().addOrUpdateBlock(fileState.getHdfsFilePath(),
                            bs.getBlockId(),
                            bs.getPrevBlockId(),
                            data.getModifiedTime(),
                            block.getSize(),
                            block.getGenerationStamp(),
                            EBlockState.Finalized,
                            txId);
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
        fileState = stateManager().updateState(fileState.getHdfsFilePath(), EFileState.Finalized);

        DFSReplicationState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            sender.send(message);
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

        DFSReplicationState rState = stateManager().get(fileState.getId());
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
