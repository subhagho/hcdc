package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.common.InvalidTransactionError;
import ai.sapper.hcdc.agents.common.ProtoBufUtils;
import ai.sapper.hcdc.agents.common.TransactionProcessor;
import ai.sapper.hcdc.agents.model.DFSBlockReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.common.utils.JSONUtils;
import ai.sapper.hcdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.hcdc.core.messaging.InvalidMessageError;
import ai.sapper.hcdc.core.messaging.MessageObject;
import ai.sapper.hcdc.core.messaging.MessageSender;
import ai.sapper.hcdc.core.model.*;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.List;
import java.util.Map;

public class SourceTransactionProcessor extends TransactionProcessor {
    private MessageSender<String, DFSChangeDelta> sender;

    public TransactionProcessor withSenderQueue(@NonNull MessageSender<String, DFSChangeDelta> sender) {
        this.sender = sender;
        return this;
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
    public void processAddFileTxMessage(DFSAddFile data,
                                        MessageObject<String, DFSChangeDelta> message,
                                        long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (data.getOverwrite()) {
            if (fileState == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("NameNode out of sync. Overwrite called, but file not present. [path=%s]",
                                data.getFile().getPath()))
                        .withFile(data.getFile());
            }
            DFSDeleteFile delF = DFSDeleteFile.newBuilder()
                    .setTransaction(data.getTransaction())
                    .setFile(data.getFile())
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    delF,
                    DFSDeleteFile.class,
                    message.value().getDomain(),
                    message.value().getEntity(),
                    MessageObject.MessageMode.Backlog);
            processDeleteFileTxMessage(delF, m, txId);
        } else if (fileState != null) {
            if (fileState.getLastTnxId() >= txId) {
                LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                        message.id(), message.mode().name()));
                return;
            } else if (!fileState.checkDeleted()) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getHdfsFilePath(),
                        String.format("Valid File already exists. [path=%s]",
                                fileState.getHdfsFilePath()))
                        .withFile(data.getFile());
            }
        }
        fileState = stateManager()
                .fileStateHelper()
                .create(data.getFile().getPath(),
                        data.getFile().getInodeId(),
                        data.getModifiedTime(),
                        data.getBlockSize(),
                        EFileState.New,
                        data.getTransaction().getTransactionId());
        List<DFSBlock> blocks = data.getBlocksList();
        if (!blocks.isEmpty()) {
            long prevBlockId = -1;
            for (DFSBlock block : blocks) {
                fileState = stateManager()
                        .fileStateHelper()
                        .addOrUpdateBlock(fileState.getHdfsFilePath(),
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
        SchemaEntity schemaEntity = isRegistered(fileState.getHdfsFilePath());
        if (schemaEntity != null) {
            DFSFileReplicaState rState = stateManager()
                    .replicaStateHelper()
                    .create(fileState.getId(),
                            fileState.getHdfsFilePath(),
                            schemaEntity,
                            true);
            rState.setSnapshotTxId(fileState.getLastTnxId());
            rState.setSnapshotTime(System.currentTimeMillis());
            rState.setSnapshotReady(true);
            rState.copyBlocks(fileState);
            rState.setSchemaLocation(fileState.getSchemaLocation());
            rState = stateManager().replicaStateHelper().update(rState);
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.update(message, schemaEntity, message.mode());
            sender.send(m);
        } else {
            LOG.debug(String.format("No match found. [path=%s]", fileState.getHdfsFilePath()));
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
    public void processAppendFileTxMessage(DFSAppendFile data,
                                           MessageObject<String, DFSChangeDelta> message,
                                           long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }
        fileState = stateManager()
                .fileStateHelper()
                .updateState(fileState.getHdfsFilePath(), EFileState.Updating);

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            DFSFile df = ProtoBufUtils.build(fileState);
            data = data.toBuilder().setFile(df).build();
            message = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    data,
                    DFSAppendFile.class,
                    rState.getEntity().getDomain(),
                    rState.getEntity().getEntity(),
                    message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]",
                            fileState.getHdfsFilePath()))
                    .withFile(data.getFile());
        } else {
            sendIgnoreTx(message, data);
        }
    }

    private void checkDirectoryRename(DFSRenameFile data,
                                      MessageObject<String, DFSChangeDelta> message,
                                      long txId) throws Exception {
        List<DFSFileState> files = stateManager()
                .fileStateHelper()
                .listFiles(data.getSrcFile().getPath());
        if (files != null && !files.isEmpty()) {
            for (DFSFileState file : files) {
                if (!file.checkDeleted()) {
                    updateFileRecursiveRename(file,
                            data.getSrcFile().getPath(),
                            data.getDestFile().getPath(),
                            message, data, txId);
                }
            }
        }
        DFSFileState fs = stateManager()
                .fileStateHelper()
                .delete(data.getSrcFile().getPath());
        sendIgnoreTx(message, data);
    }

    private void updateFileRecursiveRename(DFSFileState fileState,
                                           String srcDir,
                                           String destDir,
                                           MessageObject<String, DFSChangeDelta> message,
                                           DFSRenameFile renameFile,
                                           long txId) throws Exception {
        if (fileState.checkDeleted()) return;
        if (!fileState.hasError()) {
            String destFile = fileState.getHdfsFilePath().replace(srcDir, destDir);
            LOG.debug(String.format("Renaming file: [source=%s {%d}][target=%s]",
                    fileState.getHdfsFilePath(), fileState.getId(), destFile));
            DFSFile destF = DFSFile.newBuilder()
                    .setFileType(fileState.getFileType().name())
                    .setPath(destFile)
                    .setInodeId(fileState.getId())
                    .build();
            DFSFile srcF = ProtoBufUtils.build(fileState);
            DFSRenameFile data = DFSRenameFile.newBuilder()
                    .setTransaction(renameFile.getTransaction())
                    .setSrcFile(srcF)
                    .setDestFile(destF)
                    .setLength(fileState.getDataSize())
                    .build();

            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    data,
                    DFSRenameFile.class,
                    null,
                    null,
                    MessageObject.MessageMode.Backlog);
            m.correlationId(message.id());
            processRenameFileTxMessage(data, m, txId);
        }
    }

    private void checkDirectoryDelete(DFSDeleteFile data,
                                      MessageObject<String, DFSChangeDelta> message,
                                      long txId) throws Exception {
        List<DFSFileState> files = stateManager()
                .fileStateHelper()
                .listFiles(data.getFile().getPath());
        if (files != null && !files.isEmpty()) {
            for (DFSFileState file : files) {
                if (!file.checkDeleted()
                        && !file.getHdfsFilePath().startsWith("/tmp")) {
                    updateFileRecursiveDelete(file, message, data, txId);
                }
            }
        }
        DFSFileState fs = stateManager()
                .fileStateHelper()
                .delete(data.getFile().getPath());
        sendIgnoreTx(message, data);
    }

    private void updateFileRecursiveDelete(DFSFileState fileState,
                                           MessageObject<String, DFSChangeDelta> message,
                                           DFSDeleteFile deleteFile,
                                           long txId) throws Exception {
        if (fileState.checkDeleted()) return;
        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            stateManager()
                    .replicaStateHelper()
                    .delete(fileState.getId());

            DFSFile df = ProtoBufUtils.build(fileState);
            DFSDeleteFile data = DFSDeleteFile.newBuilder()
                    .setTransaction(deleteFile.getTransaction())
                    .setFile(df)
                    .setTimestamp(deleteFile.getTimestamp())
                    .build();

            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    data,
                    DFSDeleteFile.class,
                    rState.getEntity().getDomain(),
                    rState.getEntity().getEntity(),
                    MessageObject.MessageMode.Backlog);
            m.correlationId(message.id());

            stateManager().replicaStateHelper().delete(rState.getInode());

            sender.send(m);
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception TODO: Handle deletion of directories.
     */
    @Override
    public void processDeleteFileTxMessage(DFSDeleteFile data,
                                           MessageObject<String, DFSChangeDelta> message,
                                           long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null) {
            if (!stateManager()
                    .fileStateHelper()
                    .checkIsDirectoryPath(data.getFile().getPath())) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                                data.getFile().getPath()))
                        .withFile(data.getFile());
            }
            checkDirectoryDelete(data, message, txId);
            return;
        }
        if (fileState.checkDeleted()) {
            LOG.warn(String.format("File already deleted. [path=%s]", fileState.getHdfsFilePath()));
            return;
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }
        fileState = stateManager()
                .fileStateHelper()
                .delete(fileState.getHdfsFilePath());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            stateManager()
                    .replicaStateHelper()
                    .delete(fileState.getId());

            DFSFile df = ProtoBufUtils.build(fileState);
            data = data.toBuilder().setFile(df).build();

            message = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    data,
                    DFSDeleteFile.class,
                    rState.getEntity().getDomain(),
                    rState.getEntity().getEntity(),
                    message.mode());

            stateManager().replicaStateHelper().delete(rState.getInode());

            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]",
                            fileState.getHdfsFilePath()))
                    .withFile(data.getFile());
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
    public void processAddBlockTxMessage(DFSAddBlock data,
                                         MessageObject<String, DFSChangeDelta> message,
                                         long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()))
                    .withFile(data.getFile());
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
        fileState = stateManager()
                .fileStateHelper()
                .addOrUpdateBlock(fileState.getHdfsFilePath(),
                        data.getLastBlock().getBlockId(),
                        lastBlockId,
                        data.getTransaction().getTimestamp(),
                        data.getLastBlock().getSize(),
                        data.getLastBlock().getGenerationStamp(),
                        EBlockState.New,
                        data.getTransaction().getTransactionId());

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            if (fileState.hasBlocks()) {
                for (DFSBlockState bs : fileState.getBlocks()) {
                    if (bs.getState() != EBlockState.New) continue;
                    rState.copyBlock(bs);
                }
            }
            rState = stateManager().replicaStateHelper().update(rState);
            DFSFile df = ProtoBufUtils.build(fileState);
            data = data.toBuilder().setFile(df).build();

            message = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    data,
                    DFSAddBlock.class,
                    rState.getEntity().getDomain(),
                    rState.getEntity().getEntity(),
                    message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]",
                            fileState.getHdfsFilePath()))
                    .withFile(data.getFile());
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
    public void processUpdateBlocksTxMessage(DFSUpdateBlocks data,
                                             MessageObject<String, DFSChangeDelta> message,
                                             long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()))
                    .withFile(data.getFile());
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
                            fileState.getHdfsFilePath()))
                    .withFile(data.getFile());
        }
        for (DFSBlock block : blocks) {
            DFSBlockState bs = fileState.get(block.getBlockId());
            if (bs == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getHdfsFilePath(),
                        String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                fileState.getHdfsFilePath(), block.getBlockId()))
                        .withFile(data.getFile());
            } else if (bs.getDataSize() != block.getSize()) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getHdfsFilePath(),
                        String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                fileState.getHdfsFilePath(), block.getBlockId()))
                        .withFile(data.getFile());
            }
            if (bs.blockIsFull()) continue;

            fileState = stateManager()
                    .fileStateHelper()
                    .updateState(fileState.getHdfsFilePath(),
                            bs.getBlockId(),
                            EBlockState.Updating);
        }
        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            if (fileState.hasBlocks()) {
                for (DFSBlockState bs : fileState.getBlocks()) {
                    if (bs.getState() != EBlockState.Updating) continue;
                    rState.copyBlock(bs);
                }
            }
            rState = stateManager().replicaStateHelper().update(rState);
            DFSFile df = ProtoBufUtils.build(fileState);
            data = data.toBuilder().setFile(df).build();

            message = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    data,
                    DFSUpdateBlocks.class,
                    rState.getEntity().getDomain(),
                    rState.getEntity().getEntity(),
                    message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()))
                    .withFile(data.getFile());
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
    public void processTruncateBlockTxMessage(DFSTruncateBlock data,
                                              MessageObject<String, DFSChangeDelta> message,
                                              long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (fileState.getLastTnxId() >= txId) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
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
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (!checkCloseTxState(fileState, message.mode(), txId)) {
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
                                    fileState.getHdfsFilePath(), block.getBlockId()))
                            .withFile(data.getFile());
                } else if (bs.canUpdate()) {
                    fileState = stateManager()
                            .fileStateHelper()
                            .addOrUpdateBlock(fileState.getHdfsFilePath(),
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
                                    fileState.getHdfsFilePath(), block.getBlockId()))
                            .withFile(data.getFile());
                } else {
                    throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("File State out of sync, block state mismatch. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()))
                            .withFile(data.getFile());
                }
            }
        }
        fileState = stateManager()
                .fileStateHelper()
                .updateState(fileState.getHdfsFilePath(), EFileState.Finalized);

        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            DFSFile df = ProtoBufUtils.build(fileState);
            if (fileState.getFileType() != EFileType.UNKNOWN) {
                rState.setFileType(fileState.getFileType());
                rState.setSchemaLocation(fileState.getSchemaLocation());
            }
            DFSCloseFile.Builder builder = data.toBuilder();
            builder.clearBlocks();

            builder.setFile(df);
            for (DFSBlock block : blocks) {
                DFSBlockReplicaState brs = rState.get(block.getBlockId());
                if (brs == null) continue;

                DFSBlock.Builder bb = block.toBuilder();
                DFSBlockState bs = fileState.get(block.getBlockId());

                BlockTransactionDelta bd = bs.delta(txId);
                if (bd == null) {
                    throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("Block State out of sync, missing transaction delta. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()))
                            .withFile(data.getFile());
                }
                bb.setStartOffset(bd.getStartOffset())
                        .setEndOffset(bd.getEndOffset())
                        .setDeltaSize(bd.getEndOffset() - bd.getStartOffset() + 1);
                builder.addBlocks(bb.build());
            }
            data = builder.build();
            rState = stateManager().replicaStateHelper().update(rState);
            message = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    data,
                    DFSCloseFile.class,
                    rState.getEntity().getDomain(),
                    rState.getEntity().getEntity(),
                    message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()))
                    .withFile(data.getFile());
        } else {
            sendIgnoreTx(message, data);
        }
    }

    @Override
    public void processRenameFileTxMessage(DFSRenameFile data,
                                           MessageObject<String, DFSChangeDelta> message,
                                           long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getSrcFile().getPath());
        if (fileState == null) {
            if (!stateManager()
                    .fileStateHelper()
                    .checkIsDirectoryPath(data.getSrcFile().getPath())) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getSrcFile().getPath(),
                        String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                                data.getSrcFile().getPath()))
                        .withFile(data.getSrcFile());
            }
            checkDirectoryRename(data, message, txId);
            return;
        }
        if (!fileState.canProcess()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getSrcFile().getPath(),
                    String.format("NameNode Replica: File state is invalid. [path=%s][state=%s]",
                            data.getSrcFile().getPath(), fileState.getState().name()))
                    .withFile(data.getSrcFile());
        }
        // Delete Existing file section
        DFSDeleteFile dms = DFSDeleteFile.newBuilder()
                .setTransaction(data.getTransaction())
                .setFile(data.getSrcFile())
                .setTimestamp(data.getTransaction().getTimestamp())
                .build();
        MessageObject<String, DFSChangeDelta> dm = ChangeDeltaSerDe.create(message.value().getNamespace(),
                dms,
                DFSDeleteFile.class,
                message.value().getDomain(),
                message.value().getEntityName(),
                MessageObject.MessageMode.New);
        processDeleteFileTxMessage(dms, dm, txId);

        // Add new file section
        DFSTransaction tx = DFSTransaction.newBuilder()
                .setOp(DFSTransaction.Operation.CLOSE)
                .setTransactionId(txId)
                .setTimestamp(System.currentTimeMillis())
                .build();
        DFSFile fa = DFSFile.newBuilder()
                .setPath(data.getDestFile().getPath())
                .setInodeId(fileState.getId())
                .setFileType(EFileType.UNKNOWN.name())
                .build();
        DFSAddFile.Builder builder = DFSAddFile.newBuilder();
        builder.setTransaction(tx)
                .setFile(fa)
                .setLength(0)
                .setBlockSize(fileState.getBlockSize())
                .setModifiedTime(data.getTransaction().getTimestamp())
                .setAccessedTime(data.getTransaction().getTimestamp())
                .setOverwrite(false);
        if (fileState.hasBlocks()) {
            for (DFSBlockState bs : fileState.getBlocks()) {
                DFSBlock b = DFSBlock.newBuilder()
                        .setBlockId(bs.getBlockId())
                        .setBlockSize(bs.getBlockSize())
                        .setStartOffset(0)
                        .setSize(bs.getDataSize())
                        .setGenerationStamp(bs.getGenerationStamp())
                        .setEndOffset(bs.getDataSize())
                        .setDeleted(false)
                        .build();
                builder.addBlocks(b);
            }
        }
        DFSAddFile ams = builder.build();
        MessageObject<String, DFSChangeDelta> am = ChangeDeltaSerDe.create(message.value().getNamespace(),
                ams,
                DFSAddFile.class,
                message.value().getDomain(),
                null,
                MessageObject.MessageMode.Snapshot);
        processAddFileTxMessage(ams, am, txId);

        // Close new file section
        DFSCloseFile cms = HDFSSnapshotProcessor.generateSnapshot(fileState, fa, txId);
        MessageObject<String, DFSChangeDelta> cm = ChangeDeltaSerDe.create(message.value().getNamespace(),
                cms,
                DFSCloseFile.class,
                message.value().getDomain(),
                null,
                MessageObject.MessageMode.Backlog);
        processCloseFileTxMessage(cms, cm, txId);
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
        sender.send(message);
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
        throw new InvalidMessageError(message.id(), data.getMessage());
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
        if (!Strings.isNullOrEmpty(te.getHdfsPath())) {
            DFSFileState fileState = stateManager()
                    .fileStateHelper()
                    .get(te.getHdfsPath());
            if (fileState != null) {
                stateManager()
                        .fileStateHelper()
                        .updateState(fileState.getHdfsFilePath(), EFileState.Error);
            }
        }
        DFSTransaction tnx = extractTransaction(data);
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
