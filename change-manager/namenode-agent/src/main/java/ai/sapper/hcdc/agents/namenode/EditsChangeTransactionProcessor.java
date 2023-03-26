package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.model.BlockTransactionDelta;
import ai.sapper.cdc.core.model.EFileType;
import ai.sapper.cdc.core.utils.SchemaEntityHelper;
import ai.sapper.hcdc.agents.common.InvalidTransactionError;
import ai.sapper.hcdc.agents.common.TransactionProcessor;
import ai.sapper.hcdc.agents.model.*;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.cdc.core.utils.ProtoUtils;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

public class EditsChangeTransactionProcessor extends TransactionProcessor {
    private MessageSender<String, DFSChangeDelta> sender;

    public EditsChangeTransactionProcessor(@NonNull String name) {
        super(name);
    }

    public TransactionProcessor withSenderQueue(@NonNull MessageSender<String, DFSChangeDelta> sender) {
        this.sender = sender;
        return this;
    }

    private void sendIgnoreTx(MessageObject<String, DFSChangeDelta> message, Object data) throws Exception {
        DFSTransaction tnx = extractTransaction(data);
        if (tnx != null) {
            SchemaEntity entity = SchemaEntityHelper.parse(message.value().getEntity());
            MessageObject<String, DFSChangeDelta> im = ChangeDeltaSerDe.createIgnoreTx(entity,
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
    public void processAddFileTxMessage(@NonNull DFSFileAdd data,
                                        @NonNull MessageObject<String, DFSChangeDelta> message,
                                        @NonNull BaseTxId txId,
                                        boolean retry) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getEntity().getEntity());
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());
        if (data.getOverwrite()) {
            if (fileState != null) {
                if (!fileState.checkDeleted()) {
                    DFSFileDelete delF = DFSFileDelete.newBuilder()
                            .setTransaction(data.getTransaction())
                            .setFile(data.getFile())
                            .setTimestamp(System.currentTimeMillis())
                            .build();
                    MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(delF,
                                    DFSFileDelete.class,
                                    schemaEntity,
                                    MessageObject.MessageMode.Forked)
                            .correlationId(message.id());

                    processDeleteFileTxMessage(delF, m, txId, retry);
                } else if (fileState.checkDeleted()) {
                    stateManager()
                            .replicaStateHelper()
                            .delete(schemaEntity, fileState.getFileInfo().getInodeId());
                }
            }
        }
        fileState = createFileState(data.getFile(),
                message,
                txId.getId(),
                data.getModifiedTime(),
                data.getBlockSize(),
                data.getBlocksList(),
                EFileState.Updating,
                EBlockState.Updating,
                retry);

        schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
        if (schemaEntity != null) {
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
            rState.setSnapshotReady(true);
            rState.copyBlocks(fileState);

            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.update(message, schemaEntity, message.mode());
            sender.send(m);

            rState = stateManager().replicaStateHelper().update(rState);
        } else {
            LOGGER.debug(getClass(), txId.getId(),
                    String.format("No match found. [path=%s]", fileState.getFileInfo().getHdfsPath()));
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
    public void processAppendFileTxMessage(@NonNull DFSFileAppend data,
                                           @NonNull MessageObject<String, DFSChangeDelta> message,
                                           @NonNull BaseTxId txId,
                                           boolean retry) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (!checkTransactionState(fileState, data.getFile(), message, txId.getId(), retry)) {
            return;
        }
        fileState = stateManager()
                .fileStateHelper()
                .updateState(fileState.getFileInfo().getHdfsPath(), EFileState.Updating);
        SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
        if (schemaEntity != null) {
            DFSFileReplicaState rState = stateManager()
                    .replicaStateHelper()
                    .get(schemaEntity, fileState.getFileInfo().getInodeId());
            if (rState == null) {
                throw new InvalidMessageError(message.id(),
                        String.format("HDFS File Not registered. [path=%s]",
                                fileState.getFileInfo().getHdfsPath()));
            }
            if (!fileState.hasError() && rState.isEnabled()) {
                DFSFile df = fileState.getFileInfo().proto();
                data = data.toBuilder().setFile(df).build();
                message = ChangeDeltaSerDe.create(data,
                        DFSFileAppend.class,
                        rState.getEntity(),
                        message.mode());
                sender.send(message);
            } else if (fileState.hasError()) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("FileSystem sync error. [path=%s]",
                                fileState.getFileInfo().getHdfsPath())))
                        .withFile(data.getFile());
            }
        } else {
            sendIgnoreTx(message, data);
        }
    }

    private void checkDirectoryRename(DFSFileRename data,
                                      MessageObject<String, DFSChangeDelta> message,
                                      BaseTxId txId) throws Exception {
        List<DFSFileState> files = stateManager()
                .fileStateHelper()
                .listFiles(data.getSrcFile().getEntity().getEntity());
        if (files != null && !files.isEmpty()) {
            for (DFSFileState file : files) {
                if (!file.checkDeleted()) {
                    updateFileRecursiveRename(file,
                            data.getSrcFile().getEntity().getEntity(),
                            data.getDestFile().getEntity().getEntity(),
                            message, data, txId);
                }
            }
        }
        DFSFileState fs = stateManager()
                .fileStateHelper()
                .delete(data.getSrcFile().getEntity().getEntity());
        sendIgnoreTx(message, data);
    }

    private void updateFileRecursiveRename(DFSFileState fileState,
                                           String srcDir,
                                           String destDir,
                                           MessageObject<String, DFSChangeDelta> message,
                                           DFSFileRename renameFile,
                                           BaseTxId txId) throws Exception {
        if (fileState.checkDeleted()) return;
        if (!fileState.hasError()) {
            String destFile = fileState.getFileInfo().getHdfsPath().replace(srcDir, destDir);
            LOGGER.debug(getClass(), txId.getId(),
                    String.format("Renaming file: [source=%s {%d}][target=%s]",
                            fileState.getFileInfo().getHdfsPath(),
                            fileState.getFileInfo().getInodeId(),
                            destFile));
            DFSFile destF = fileState.getFileInfo().proto(destFile);
            DFSFile srcF = fileState.getFileInfo().proto();
            DFSFileRename data = DFSFileRename.newBuilder()
                    .setTransaction(renameFile.getTransaction())
                    .setSrcFile(srcF)
                    .setDestFile(destF)
                    .setLength(fileState.getDataSize())
                    .build();

            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(data,
                            DFSFileRename.class,
                            SchemaEntityHelper.parse(srcF.getEntity()),
                            MessageObject.MessageMode.Recursive)
                    .correlationId(message.id());

            processRenameFileTxMessage(data, m, txId, false);
        }
    }

    private void checkDirectoryDelete(DFSFileDelete data,
                                      MessageObject<String, DFSChangeDelta> message,
                                      BaseTxId txId) throws Exception {
        List<DFSFileState> files = stateManager()
                .fileStateHelper()
                .listFiles(data.getFile().getEntity().getEntity());
        boolean delete = true;
        if (files != null && !files.isEmpty()) {
            for (DFSFileState file : files) {
                if (!file.checkDeleted()
                        && !file.getFileInfo().getHdfsPath().startsWith("/tmp")) {
                    delete = updateFileRecursiveDelete(file, message, data, txId);
                }
            }
        }
        if (delete) {
            DFSFileState fs = stateManager()
                    .fileStateHelper()
                    .delete(data.getFile().getEntity().getEntity());
        }
        sendIgnoreTx(message, data);
    }

    private boolean updateFileRecursiveDelete(DFSFileState fileState,
                                              MessageObject<String, DFSChangeDelta> message,
                                              DFSFileDelete deleteFile,
                                              BaseTxId txId) throws Exception {
        SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
        if (schemaEntity == null) {
            LOGGER.warn(getClass(), txId.getId(),
                    String.format("HDFS File Not registered. [path=%s]", fileState.getFileInfo().getHdfsPath()));
            return true;
        }
        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState != null) {
            DFSFile df = fileState.getFileInfo().proto();
            DFSFileDelete data = DFSFileDelete.newBuilder()
                    .setTransaction(deleteFile.getTransaction())
                    .setFile(df)
                    .setTimestamp(deleteFile.getTimestamp())
                    .build();

            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(data,
                            DFSFileDelete.class,
                            rState.getEntity(),
                            MessageObject.MessageMode.Recursive)
                    .correlationId(message.id());

            sender.send(m);

            stateManager()
                    .replicaStateHelper()
                    .delete(schemaEntity, rState.getFileInfo().getInodeId());
        }
        return true;
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
            if (!stateManager()
                    .fileStateHelper()
                    .checkIsDirectoryPath(path)) {
                if (retry) return;
                if (path.startsWith("/tmp/")) { // TODO: Read temp directory from config
                    DefaultLogger.warn(LOG, String.format("Temp file not registered. [path=%s]", path));
                    return;
                }
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        path,
                        new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                                path)))
                        .withFile(data.getFile());
            }
            checkDirectoryDelete(data, message, txId);
            return;
        }
        if (fileState.checkDeleted()) {
            LOGGER.warn(getClass(), txId.getId(),
                    String.format("File already deleted. [path=%s]", fileState.getFileInfo().getHdfsPath()));
            return;
        }
        if (fileState.getLastTnxId() >= txId.getId()) {
            LOGGER.warn(getClass(), txId.getId(),
                    String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                            message.id(), message.mode().name()));
            return;
        }
        SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
        if (schemaEntity == null) {
            sendIgnoreTx(message, data);
        } else {
            DFSFileReplicaState rState = stateManager()
                    .replicaStateHelper()
                    .get(schemaEntity, fileState.getFileInfo().getInodeId());
            if (rState == null && retry) {
                fileState = stateManager()
                        .fileStateHelper()
                        .delete(fileState.getFileInfo().getHdfsPath());
                return;
            }
            if (!fileState.hasError() && rState != null && rState.isEnabled()) {
                stateManager()
                        .replicaStateHelper()
                        .delete(schemaEntity, fileState.getFileInfo().getInodeId());

                DFSFile df = fileState.getFileInfo().proto();
                data = data.toBuilder().setFile(df).build();

                message = ChangeDeltaSerDe.create(data,
                        DFSFileDelete.class,
                        rState.getEntity(),
                        message.mode());

                sender.send(message);

                stateManager().replicaStateHelper().delete(schemaEntity, rState.getFileInfo().getInodeId());
            } else if (fileState.hasError()) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("FileSystem sync error. [path=%s]",
                                fileState.getFileInfo().getHdfsPath())))
                        .withFile(data.getFile());
            }
        }
        fileState = stateManager()
                .fileStateHelper()
                .delete(fileState.getFileInfo().getHdfsPath());
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
        if (!checkTransactionState(fileState, data.getFile(), message, txId.getId(), retry)) {
            return;
        }
        long lastBlockId = -1;
        if (data.hasPenultimateBlock()) {
            lastBlockId = data.getPenultimateBlock().getBlockId();
        }
        DFSBlockState blockState = null;
        if (retry) {
            if (fileState.hasBlocks()) {
                blockState = fileState.get(data.getLastBlock().getBlockId());
            }
        }
        if (blockState == null)
            fileState = stateManager()
                    .fileStateHelper()
                    .addOrUpdateBlock(fileState.getFileInfo().getHdfsPath(),
                            data.getLastBlock().getBlockId(),
                            lastBlockId,
                            data.getTransaction().getTimestamp(),
                            data.getLastBlock().getSize(),
                            data.getLastBlock().getGenerationStamp(),
                            EBlockState.New,
                            data.getTransaction().getId());

        SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
        if (schemaEntity == null) {
            sendIgnoreTx(message, data);
        } else {
            DFSFileReplicaState rState = stateManager()
                    .replicaStateHelper()
                    .get(schemaEntity, fileState.getFileInfo().getInodeId());
            if (rState == null) {
                throw new InvalidMessageError(message.id(),
                        String.format("HDFS File Not registered. [path=%s]",
                                fileState.getFileInfo().getHdfsPath()));
            }
            if (!fileState.hasError() && rState.isEnabled()) {
                if (fileState.hasBlocks()) {
                    for (DFSBlockState bs : fileState.getBlocks()) {
                        if (bs.getState() != EBlockState.New) continue;
                        rState.copyBlock(bs);
                    }
                }
                DFSFile df = fileState.getFileInfo().proto();
                data = data.toBuilder().setFile(df).build();

                message = ChangeDeltaSerDe.create(data,
                        DFSBlockAdd.class,
                        rState.getEntity(),
                        message.mode());
                sender.send(message);

                rState = stateManager().replicaStateHelper().update(rState);
            } else if (fileState.hasError()) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("File State is in error. [path=%s]",
                                fileState.getFileInfo().getHdfsPath())))
                        .withFile(data.getFile());
            }
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
        if (!checkTransactionState(fileState, data.getFile(), message, txId.getId(), retry)) {
            return;
        }
        List<DFSBlock> blocks = data.getBlocksList();
        if (blocks.isEmpty()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(String.format("File State out of sync, no block to update. [path=%s]",
                            fileState.getFileInfo().getHdfsPath())))
                    .withFile(data.getFile());
        }
        for (DFSBlock block : blocks) {
            DFSBlockState bs = fileState.get(block.getBlockId());
            if (bs == null) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                        .withFile(data.getFile());
            } else if (bs.getDataSize() != block.getSize()) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                        .withFile(data.getFile());
            }
            if (bs.blockIsFull()) continue;

            fileState = stateManager()
                    .fileStateHelper()
                    .updateState(fileState.getFileInfo().getHdfsPath(),
                            bs.getBlockId(),
                            EBlockState.Updating);
        }
        SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
        if (schemaEntity == null) {
            sendIgnoreTx(message, data);
        } else {
            DFSFileReplicaState rState = stateManager()
                    .replicaStateHelper()
                    .get(schemaEntity, fileState.getFileInfo().getInodeId());
            if (rState == null) {
                throw new InvalidMessageError(message.id(),
                        String.format("HDFS File Not registered. [path=%s]",
                                fileState.getFileInfo().getHdfsPath()));
            }
            if (!fileState.hasError() && rState.isEnabled()) {
                if (fileState.hasBlocks()) {
                    for (DFSBlockState bs : fileState.getBlocks()) {
                        if (bs.getState() != EBlockState.Updating) continue;
                        rState.copyBlock(bs);
                    }
                }
                DFSFile df = fileState.getFileInfo().proto();
                data = data.toBuilder().setFile(df).build();

                message = ChangeDeltaSerDe.create(data,
                        DFSBlockUpdate.class,
                        rState.getEntity(),
                        message.mode());
                sender.send(message);

                rState = stateManager().replicaStateHelper().update(rState);
            } else if (fileState.hasError()) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("FileSystem sync error. [path=%s]",
                                fileState.getFileInfo().getHdfsPath())))
                        .withFile(data.getFile());
            }
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
        if (!checkTransactionState(fileState, data.getFile(), message, txId.getId(), retry)) {
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
    public void processCloseFileTxMessage(@NonNull DFSFileClose data,
                                          @NonNull MessageObject<String, DFSChangeDelta> message,
                                          @NonNull BaseTxId txId,
                                          boolean retry) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (!checkTransactionState(fileState, data.getFile(), message, txId.getId(), retry)) {
            return;
        }
        List<DFSBlock> blocks = data.getBlocksList();
        if (!blocks.isEmpty()) {
            for (DFSBlock block : blocks) {
                DFSBlockState bs = fileState.get(block.getBlockId());
                if (bs == null) {
                    throw new InvalidTransactionError(txId.getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                            .withFile(data.getFile());
                } else if (bs.canUpdate()) {
                    fileState = stateManager()
                            .fileStateHelper()
                            .addOrUpdateBlock(fileState.getFileInfo().getHdfsPath(),
                                    bs.getBlockId(),
                                    bs.getPrevBlockId(),
                                    data.getModifiedTime(),
                                    block.getSize(),
                                    block.getGenerationStamp(),
                                    EBlockState.Finalized,
                                    txId.getId());
                } else if (bs.getDataSize() != block.getSize()) {
                    throw new InvalidTransactionError(txId.getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                            .withFile(data.getFile());
                } else {
                    throw new InvalidTransactionError(txId.getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(String.format("File State out of sync, block state mismatch. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                            .withFile(data.getFile());
                }
            }
        }
        fileState = stateManager()
                .fileStateHelper()
                .updateState(fileState.getFileInfo().getHdfsPath(), EFileState.Finalized);
        SchemaEntity schemaEntity = isRegistered(fileState.getFileInfo().getHdfsPath());
        if (schemaEntity == null) {
            sendIgnoreTx(message, data);
        } else {
            DFSFileReplicaState rState = stateManager()
                    .replicaStateHelper()
                    .get(schemaEntity, fileState.getFileInfo().getInodeId());
            if (rState == null) {
                throw new InvalidMessageError(message.id(),
                        String.format("HDFS File Not registered. [path=%s]",
                                fileState.getFileInfo().getHdfsPath()));
            }
            if (!fileState.hasError() && rState.isEnabled()) {
                DFSFile df = fileState.getFileInfo().proto();

                DFSFileClose.Builder builder = data.toBuilder();
                builder.clearBlocks();

                builder.setFile(df);
                for (DFSBlock block : blocks) {
                    DFSBlockReplicaState brs = rState.get(block.getBlockId());
                    if (brs == null) continue;

                    DFSBlock.Builder bb = block.toBuilder();
                    DFSBlockState bs = fileState.get(block.getBlockId());

                    BlockTransactionDelta bd = bs.delta(txId.getId());
                    if (bd == null) {
                        throw new InvalidTransactionError(txId.getId(),
                                DFSError.ErrorCode.SYNC_STOPPED,
                                fileState.getFileInfo().getHdfsPath(),
                                new Exception(String.format("Block State out of sync, missing transaction delta. [path=%s][blockID=%d]",
                                        fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                                .withFile(data.getFile());
                    }
                    bb.setStartOffset(bd.getStartOffset())
                            .setEndOffset(bd.getEndOffset())
                            .setDeltaSize(bd.getEndOffset() - bd.getStartOffset() + 1);
                    builder.addBlocks(bb.build());
                }
                data = builder.build();
                message = ChangeDeltaSerDe.create(data,
                        DFSFileClose.class,
                        rState.getEntity(),
                        message.mode());
                sender.send(message);

                rState = stateManager().replicaStateHelper().update(rState);
            } else if (fileState.hasError()) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("FileSystem sync error. [path=%s]", fileState.getFileInfo().getHdfsPath())))
                        .withFile(data.getFile());
            }
        }
    }

    @Override
    public void processRenameFileTxMessage(@NonNull DFSFileRename data,
                                           @NonNull MessageObject<String, DFSChangeDelta> message,
                                           @NonNull BaseTxId txId,
                                           boolean retry) throws Exception {
        String path = data.getSrcFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState == null) {
            if (!stateManager()
                    .fileStateHelper()
                    .checkIsDirectoryPath(path) && !retry) {
                throw new InvalidTransactionError(txId.getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        path,
                        new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                                path)))
                        .withFile(data.getSrcFile());
            }
            checkDirectoryRename(data, message, txId);
            return;
        }
        if (!fileState.canProcess()) {
            throw new InvalidTransactionError(txId.getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    path,
                    new Exception(String.format("NameNode Replica: File state is invalid. [path=%s][state=%s]",
                            path, fileState.getState().name())))
                    .withFile(data.getSrcFile());
        }
        SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());

        // Add new file section
        DFSTransaction tx = ProtoUtils.buildTx(txId, DFSTransaction.Operation.CLOSE);
        DFSFile fa = DFSFile.newBuilder()
                .setEntity(data.getDestFile().getEntity())
                .setInodeId(fileState.getFileInfo().getInodeId())
                .setFileType(EFileType.UNKNOWN.name())
                .build();
        DFSFileAdd.Builder builder = DFSFileAdd.newBuilder();
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
        DFSFileAdd ams = builder.build();
        SchemaEntity tEntity = SchemaEntityHelper.parse(ams.getFile().getEntity());
        MessageObject<String, DFSChangeDelta> am = ChangeDeltaSerDe.create(ams,
                        DFSFileAdd.class,
                        tEntity,
                        MessageObject.MessageMode.Snapshot)
                .correlationId(message.id());

        processAddFileTxMessage(ams, am, txId, retry);

        // Close new file section
        DFSFileClose cms = HDFSSnapshotProcessor.generateSnapshot(fileState, fa, txId);
        MessageObject<String, DFSChangeDelta> cm = ChangeDeltaSerDe.create(cms,
                        DFSFileClose.class,
                        tEntity,
                        MessageObject.MessageMode.Backlog)
                .correlationId(message.id());
        processCloseFileTxMessage(cms, cm, txId, retry);

        // Delete Existing file section
        DFSFileDelete dms = DFSFileDelete.newBuilder()
                .setTransaction(data.getTransaction())
                .setFile(data.getSrcFile())
                .setTimestamp(data.getTransaction().getTimestamp())
                .build();
        MessageObject<String, DFSChangeDelta> dm = ChangeDeltaSerDe.create(dms,
                        DFSFileDelete.class,
                        schemaEntity,
                        MessageObject.MessageMode.Forked)
                .correlationId(message.id());

        processDeleteFileTxMessage(dms, dm, txId, retry);
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
        sender.send(message);
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
        throw new InvalidMessageError(message.id(), data.getMessage());
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
                            InvalidTransactionError te) throws Exception {
        if (!Strings.isNullOrEmpty(te.getHdfsPath())) {
            DFSFileState fileState = stateManager()
                    .fileStateHelper()
                    .get(te.getHdfsPath());
            if (fileState != null) {
                stateManager()
                        .fileStateHelper()
                        .updateState(fileState.getFileInfo().getHdfsPath(), EFileState.Error);
            }
        }
        DFSTransaction tnx = extractTransaction(data);
        if (tnx != null) {
            SchemaEntity entity = null;
            if (te.getFile() != null) {
                entity = SchemaEntityHelper.parse(te.getFile().getEntity());
            } else {
                entity = SchemaEntityHelper.parse(message.value().getEntity());
            }
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.createErrorTx(message.id(),
                    tnx,
                    te.getErrorCode(),
                    te.getMessage(),
                    entity,
                    te.getFile());
            sender.send(m);
        }
    }
}
