package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.hcdc.agents.common.InvalidTransactionError;
import ai.sapper.hcdc.agents.common.ProtoBufUtils;
import ai.sapper.hcdc.agents.common.TransactionProcessor;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import ai.sapper.hcdc.agents.model.EBlockState;
import ai.sapper.hcdc.agents.model.EFileState;
import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

public class EntityChangeTransactionProcessor extends TransactionProcessor {
    private MessageSender<String, DFSChangeDelta> sender;

    public EntityChangeTransactionProcessor(@NonNull String name) {
        super(name);
    }

    public TransactionProcessor withSenderQueue(@NonNull MessageSender<String, DFSChangeDelta> sender) {
        this.sender = sender;
        return this;
    }

    private void sendIgnoreTx(MessageObject<String, DFSChangeDelta> message, Object data) throws Exception {
        // Should not be called...
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAddFileTxMessage(DFSAddFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState != null) {
            if (!fileState.checkDeleted()) {
                if (txId > fileState.getLastTnxId()) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            String.format("Valid File already exists. [path=%s]",
                                    fileState.getFileInfo().getHdfsPath()))
                            .withFile(data.getFile());
                } else {
                    LOGGER.warn(getClass(), txId,
                            String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                                    message.id(), message.mode().name()));
                    return;
                }
            }
        }

        fileState = stateManager()
                .fileStateHelper()
                .create(data.getFile(),
                        data.getModifiedTime(),
                        data.getBlockSize(),
                        EFileState.New,
                        data.getTransaction().getTransactionId());
        if (ProtoBufUtils.update(fileState, data.getFile())) {
            fileState = stateManager()
                    .fileStateHelper()
                    .update(fileState);
        }
        List<DFSBlock> blocks = data.getBlocksList();
        if (!blocks.isEmpty()) {
            long prevBlockId = -1;
            for (DFSBlock block : blocks) {
                fileState = stateManager()
                        .fileStateHelper()
                        .addOrUpdateBlock(fileState.getFileInfo().getHdfsPath(),
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
        fileState.setState(EFileState.Updating);

        sender.send(message);
        fileState = stateManager()
                .fileStateHelper()
                .update(fileState);
    }

    private void addFile(DFSCloseFile data,
                         MessageObject<String, DFSChangeDelta> message,
                         long txId) throws Exception {
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState != null) {
            if (fileState.getLastTnxId() >= txId) {
                LOGGER.warn(getClass(), txId,
                        String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                                message.id(), message.mode().name()));
                return;
            } else if (!fileState.checkDeleted()) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        String.format("Valid File already exists. [path=%s]",
                                fileState.getFileInfo().getHdfsPath()))
                        .withFile(data.getFile());
            }
        }
        fileState = stateManager()
                .fileStateHelper()
                .create(data.getFile(),
                        data.getModifiedTime(),
                        data.getBlockSize(),
                        EFileState.New,
                        data.getTransaction().getTransactionId());
        if (ProtoBufUtils.update(fileState, data.getFile())) {
            fileState = stateManager()
                    .fileStateHelper()
                    .update(fileState);
        }
        List<DFSBlock> blocks = data.getBlocksList();
        if (!blocks.isEmpty()) {
            long prevBlockId = -1;
            for (DFSBlock block : blocks) {
                fileState = stateManager()
                        .fileStateHelper()
                        .addOrUpdateBlock(fileState.getFileInfo().getHdfsPath(),
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
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (fileState.getLastTnxId() >= txId) {
            LOGGER.warn(getClass(), txId,
                    String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                            message.id(), message.mode().name()));
            return;
        }

        ProtoBufUtils.update(fileState, data.getFile());
        fileState.setState(EFileState.Updating);

        sender.send(message);
        fileState = stateManager()
                .fileStateHelper()
                .update(fileState);
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
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (fileState.getLastTnxId() >= txId) {
            LOGGER.warn(getClass(), txId,
                    String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                            message.id(), message.mode().name()));
            return;
        }

        sender.send(message);
        if (message.mode() == MessageObject.MessageMode.Forked) {
            fileState = stateManager()
                    .fileStateHelper()
                    .delete(fileState.getFileInfo().getHdfsPath());
        } else {
            fileState = stateManager()
                    .fileStateHelper()
                    .markDeleted(fileState.getFileInfo().getHdfsPath(), true);
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
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (fileState.getLastTnxId() >= txId) {
            LOGGER.warn(getClass(), txId,
                    String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                            message.id(), message.mode().name()));
            return;
        }
        long lastBlockId = -1;
        if (data.hasPenultimateBlock()) {
            lastBlockId = data.getPenultimateBlock().getBlockId();
        }
        fileState = stateManager()
                .fileStateHelper()
                .addOrUpdateBlock(fileState.getFileInfo().getHdfsPath(),
                        data.getLastBlock().getBlockId(),
                        lastBlockId,
                        data.getTransaction().getTimestamp(),
                        data.getLastBlock().getSize(),
                        data.getLastBlock().getGenerationStamp(),
                        EBlockState.New,
                        data.getTransaction().getTransactionId());
        sender.send(message);
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
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (fileState.getLastTnxId() >= txId) {
            LOGGER.warn(getClass(), txId,
                    String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                            message.id(), message.mode().name()));
            return;
        }
        List<DFSBlock> blocks = data.getBlocksList();
        if (blocks.isEmpty()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    String.format("File State out of sync, no block to update. [path=%s]",
                            fileState.getFileInfo().getHdfsPath()))
                    .withFile(data.getFile());
        }
        for (DFSBlock block : blocks) {
            DFSBlockState bs = fileState.get(block.getBlockId());
            if (bs == null) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                fileState.getFileInfo().getHdfsPath(), block.getBlockId()))
                        .withFile(data.getFile());
            } else if (bs.getDataSize() != block.getSize()) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                fileState.getFileInfo().getHdfsPath(), block.getBlockId()))
                        .withFile(data.getFile());
            }
            if (bs.blockIsFull()) continue;

            stateManager()
                    .fileStateHelper()
                    .updateState(fileState.getFileInfo().getHdfsPath(),
                            bs.getBlockId(),
                            EBlockState.Updating);
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
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (fileState.getLastTnxId() >= txId) {
            LOGGER.warn(getClass(), txId,
                    String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
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
    public void processCloseFileTxMessage(DFSCloseFile data,
                                          MessageObject<String, DFSChangeDelta> message,
                                          long txId) throws Exception {
        if (message.mode() == MessageObject.MessageMode.Snapshot) {
            addFile(data, message, txId);
        }
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()))
                    .withFile(data.getFile());
        }
        if (!checkCloseTxState(fileState, message.mode(), txId)) {
            LOGGER.warn(getClass(), txId,
                    String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                            message.id(), message.mode().name()));
            return;
        }

        List<DFSBlock> blocks = data.getBlocksList();
        if (!blocks.isEmpty()) {
            for (DFSBlock block : blocks) {
                DFSBlockState bs = fileState.get(block.getBlockId());
                if (bs == null) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId()))
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
                                    txId);
                } else if (bs.getDataSize() != block.getSize()) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId()))
                            .withFile(data.getFile());
                } else {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            String.format("File State out of sync, block state mismatch. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId()))
                            .withFile(data.getFile());
                }
            }
        }
        fileState.setState(EFileState.Finalized);

        sender.send(message);
        fileState = stateManager()
                .fileStateHelper()
                .update(fileState);
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
                fileState.setState(EFileState.Error);
                if (tnx != null) {
                    fileState.setLastTnxId(tnx.getTransactionId());
                }
                fileState.setTimestamp(System.currentTimeMillis());
                stateManager().fileStateHelper().update(fileState);
            }
        }
        LOGGER.warn(getClass(), txId,
                String.format("Received Error Message: %s. [TX=%d][ERROR CODE=%s]",
                        data.getMessage(), txId, data.getCode().name()));
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
                        .updateState(fileState.getFileInfo().getHdfsPath(), EFileState.Error);
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
