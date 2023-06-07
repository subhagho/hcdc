package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.HCdcStateManager;
import ai.sapper.cdc.core.InvalidTransactionError;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.model.HCdcProcessingState;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.utils.ProtoUtils;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.core.model.dfs.EBlockState;
import ai.sapper.cdc.core.model.EFileState;
import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public abstract class TransactionProcessor {
    public static final Logger LOG = LoggerFactory.getLogger(TransactionProcessor.class);

    private final String name;
    private HCdcStateManager stateManager;
    private HCdcSchemaManager schemaManager;
    private MessageSender<String, DFSChangeDelta> errorSender;

    public TransactionProcessor(@NonNull String name) {
        this.name = name;
    }

    public TransactionProcessor withStateManager(@NonNull HCdcStateManager stateManager) {
        this.stateManager = stateManager;
        return this;
    }

    public TransactionProcessor withErrorQueue(@NonNull MessageSender<String, DFSChangeDelta> errorSender) {
        this.errorSender = errorSender;
        return this;
    }

    public TransactionProcessor withSchemaManager(@NonNull HCdcSchemaManager schemaManager) {
        this.schemaManager = schemaManager;
        return this;
    }

    public abstract void processAddFileTxMessage(@NonNull DFSFileAdd data,
                                                 @NonNull MessageObject<String, DFSChangeDelta> message,
                                                 @NonNull HCdcTxId txId,
                                                 boolean retry) throws Exception;

    public SchemaEntity isRegistered(String hdfsPath) throws Exception {
        Preconditions.checkNotNull(schemaManager);
        return schemaManager.matches(hdfsPath);
    }

    public abstract void processAppendFileTxMessage(@NonNull DFSFileAppend data,
                                                    @NonNull MessageObject<String, DFSChangeDelta> message,
                                                    @NonNull HCdcTxId txId,
                                                    boolean retry) throws Exception;

    public abstract void processDeleteFileTxMessage(@NonNull DFSFileDelete data,
                                                    @NonNull MessageObject<String, DFSChangeDelta> message,
                                                    @NonNull HCdcTxId txId,
                                                    boolean retry) throws Exception;

    public abstract void processAddBlockTxMessage(@NonNull DFSBlockAdd data,
                                                  @NonNull MessageObject<String, DFSChangeDelta> message,
                                                  @NonNull HCdcTxId txId,
                                                  boolean retry) throws Exception;

    public abstract void processUpdateBlocksTxMessage(@NonNull DFSBlockUpdate data,
                                                      @NonNull MessageObject<String, DFSChangeDelta> message,
                                                      @NonNull HCdcTxId txId,
                                                      boolean retry) throws Exception;

    public abstract void processTruncateBlockTxMessage(@NonNull DFSBlockTruncate data,
                                                       @NonNull MessageObject<String, DFSChangeDelta> message,
                                                       @NonNull HCdcTxId txId,
                                                       boolean retry) throws Exception;

    public abstract void processCloseFileTxMessage(@NonNull DFSFileClose data,
                                                   @NonNull MessageObject<String, DFSChangeDelta> message,
                                                   @NonNull HCdcTxId txId,
                                                   boolean retry) throws Exception;

    public abstract void processRenameFileTxMessage(@NonNull DFSFileRename data,
                                                    @NonNull MessageObject<String, DFSChangeDelta> message,
                                                    @NonNull HCdcTxId txId,
                                                    boolean retry) throws Exception;

    public abstract void processIgnoreTxMessage(@NonNull DFSIgnoreTx data,
                                                @NonNull MessageObject<String, DFSChangeDelta> message,
                                                @NonNull HCdcTxId txId) throws Exception;

    public abstract void processErrorTxMessage(@NonNull DFSError data,
                                               @NonNull MessageObject<String, DFSChangeDelta> message,
                                               @NonNull HCdcTxId txId) throws Exception;

    public abstract void handleError(@NonNull MessageObject<String, DFSChangeDelta> message,
                                     @NonNull Object data,
                                     @NonNull InvalidTransactionError te) throws Exception;

    public void updateTransaction(@NonNull HCdcTxId txId,
                                  @NonNull MessageObject<String, DFSChangeDelta> message) throws Exception {
        if (message.mode() == MessageObject.MessageMode.New
                && txId.getId() > 0) {
            stateManager().update(txId);
            LOGGER.debug(getClass(),
                    txId.getId(),
                    String.format("Processed transaction delta. [TXID=%s]", txId.asString()));
        }
    }

    public boolean checkTransactionState(DFSFileState fileState,
                                         @NonNull DFSFile file,
                                         @NonNull MessageObject<String, DFSChangeDelta> message,
                                         long txId, boolean retry) throws InvalidTransactionError {
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(txId,
                    DFSError.ErrorCode.SYNC_STOPPED,
                    file.getEntity().getEntity(),
                    new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            file.getEntity().getEntity())))
                    .withFile(file);
        }
        if (message.mode() == MessageObject.MessageMode.New) {
            if (fileState.getLastTnxId() >= txId && !retry) {
                LOGGER.warn(getClass(), txId,
                        String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                                message.id(), message.mode().name()));
                return false;
            }
        }
        return true;
    }

    public DFSTransaction extractTransaction(Object data) {
        if (data instanceof DFSFileAdd) {
            return ((DFSFileAdd) data).getTransaction();
        } else if (data instanceof DFSFileAppend) {
            return ((DFSFileAppend) data).getTransaction();
        } else if (data instanceof DFSFileDelete) {
            return ((DFSFileDelete) data).getTransaction();
        } else if (data instanceof DFSBlockAdd) {
            return ((DFSBlockAdd) data).getTransaction();
        } else if (data instanceof DFSBlockUpdate) {
            return ((DFSBlockUpdate) data).getTransaction();
        } else if (data instanceof DFSBlockTruncate) {
            return ((DFSBlockTruncate) data).getTransaction();
        } else if (data instanceof DFSFileClose) {
            return ((DFSFileClose) data).getTransaction();
        } else if (data instanceof DFSFileRename) {
            return ((DFSFileRename) data).getTransaction();
        } else if (data instanceof DFSIgnoreTx) {
            return ((DFSIgnoreTx) data).getTransaction();
        } else if (data instanceof DFSError) {
            return ((DFSError) data).getTransaction();
        }
        return null;
    }

    public void processTxMessage(@NonNull MessageObject<String, DFSChangeDelta> message,
                                 @NonNull Object data,
                                 @NonNull HCdcTxId txId,
                                 boolean retry) throws Exception {
        if (data instanceof DFSFileAdd) {
            processAddFileTxMessage((DFSFileAdd) data, message, txId, retry);
        } else if (data instanceof DFSFileAppend) {
            processAppendFileTxMessage((DFSFileAppend) data, message, txId, retry);
        } else if (data instanceof DFSFileDelete) {
            processDeleteFileTxMessage((DFSFileDelete) data, message, txId, retry);
        } else if (data instanceof DFSBlockAdd) {
            processAddBlockTxMessage((DFSBlockAdd) data, message, txId, retry);
        } else if (data instanceof DFSBlockUpdate) {
            processUpdateBlocksTxMessage((DFSBlockUpdate) data, message, txId, retry);
        } else if (data instanceof DFSBlockTruncate) {
            processTruncateBlockTxMessage((DFSBlockTruncate) data, message, txId, retry);
        } else if (data instanceof DFSFileClose) {
            processCloseFileTxMessage((DFSFileClose) data, message, txId, retry);
        } else if (data instanceof DFSFileRename) {
            processRenameFileTxMessage((DFSFileRename) data, message, txId, retry);
        } else if (data instanceof DFSIgnoreTx) {
            processIgnoreTxMessage((DFSIgnoreTx) data, message, txId);
        } else if (data instanceof DFSError) {
            processErrorTxMessage((DFSError) data, message, txId);
        } else {
            throw new InvalidMessageError(message.id(), String.format("Message Body type not supported. [type=%s]", data.getClass().getCanonicalName()));
        }
    }

    public HCdcTxId checkMessageSequence(MessageObject<String, DFSChangeDelta> message,
                                         boolean ignoreMissing,
                                         boolean retry) throws Exception {
        HCdcTxId txId = ProtoUtils.fromTx(message.value().getTx());
        if (message.mode() == MessageObject.MessageMode.New) {
            HCdcProcessingState txState = (HCdcProcessingState) stateManager().processingState();
            long offset = txState.getProcessedOffset().getId();
            if (offset < 0) {
                return txId;
            }
            if (txId.getId() != offset + 1) {
                if (!ignoreMissing) {
                    throw new InvalidMessageError(message.id(),
                            String.format("Detected missing transaction. [expected=%d][current=%d]",
                                    offset + 1, txId.getId()));
                }
            }
            if (txId.compare(txState.getProcessedOffset(), false) <= 0) {
                if (retry) {
                    return txId;
                }
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message: Transaction already processed. [TXID=%d][CURRENT=%d]",
                                txId.getId(), offset));

            }
        }
        return txId;
    }

    public DFSFileState createFileState(@NonNull DFSFile file,
                                        @NonNull MessageObject<String, DFSChangeDelta> message,
                                        long txId,
                                        long updateTime,
                                        long blockSize,
                                        List<DFSBlock> blocks,
                                        @NonNull EFileState fState,
                                        @NonNull EBlockState bState,
                                        boolean retry) throws Exception {
        String path = file.getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState != null) {
            if (!retry) {
                if (fileState.getLastTnxId() >= txId &&
                        message.mode() == MessageObject.MessageMode.New) {
                    LOGGER.warn(getClass(), txId,
                            String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                                    message.id(), message.mode().name()));
                    return null;
                } else if (!fileState.checkDeleted()) {
                    throw new InvalidTransactionError(txId,
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(String.format("Valid File already exists. [path=%s]",
                                    fileState.getFileInfo().getHdfsPath())))
                            .withFile(file);
                }
            } else if (fileState.getLastTnxId() != txId) {
                throw new InvalidTransactionError(txId,
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(
                                String.format("Invalid processed transaction. [path=%s][expected tx=%d][processed tx=%d]",
                                        fileState.getFileInfo().getHdfsPath(), txId, fileState.getLastTnxId())))
                        .withFile(file);
            }
        }
        if (fileState != null) {
            stateManager()
                    .fileStateHelper()
                    .delete(fileState.getFileInfo().getHdfsPath());
        }

        fileState = stateManager()
                .fileStateHelper()
                .create(file,
                        updateTime,
                        blockSize,
                        EFileState.New,
                        txId);
        if (ProtoBufUtils.update(fileState, file)) {
            fileState = stateManager()
                    .fileStateHelper()
                    .update(fileState);
        }
        if (!blocks.isEmpty()) {
            long prevBlockId = -1;
            for (DFSBlock block : blocks) {
                DFSBlockState bs = null;
                if (retry) {
                    bs = fileState.get(block.getBlockId());
                }
                if (bs == null)
                    fileState = stateManager()
                            .fileStateHelper()
                            .addOrUpdateBlock(fileState.getFileInfo().getHdfsPath(),
                                    block.getBlockId(),
                                    prevBlockId,
                                    updateTime,
                                    block.getSize(),
                                    block.getGenerationStamp(),
                                    bState,
                                    txId);
                prevBlockId = block.getBlockId();
            }
        }
        fileState.setState(fState);

        return fileState;
    }
}
