package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.model.SchemaEntity;
import ai.sapper.cdc.core.filters.DomainManager;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.model.DFSFileState;
import ai.sapper.hcdc.agents.model.AgentTxState;
import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Preconditions;
import com.google.protobuf.MessageOrBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public abstract class TransactionProcessor {
    public static final Logger LOG = LoggerFactory.getLogger(TransactionProcessor.class);

    private ZkStateManager stateManager;
    private MessageSender<String, DFSChangeDelta> errorSender;

    public TransactionProcessor withStateManager(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;

        return this;
    }

    public TransactionProcessor withErrorQueue(@NonNull MessageSender<String, DFSChangeDelta> errorSender) {
        this.errorSender = errorSender;
        return this;
    }

    public abstract void processAddFileTxMessage(DFSAddFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public SchemaEntity isRegistered(String hdfsPath) throws Exception {
        Preconditions.checkState(stateManager instanceof ProcessorStateManager);
        DomainManager dm = ((ProcessorStateManager) stateManager).domainManager();

        return dm.matches(hdfsPath);
    }

    public abstract void processAppendFileTxMessage(DFSAppendFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void processDeleteFileTxMessage(DFSDeleteFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void processAddBlockTxMessage(DFSAddBlock data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void processUpdateBlocksTxMessage(DFSUpdateBlocks data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void processTruncateBlockTxMessage(DFSTruncateBlock data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void processCloseFileTxMessage(DFSCloseFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void processRenameFileTxMessage(DFSRenameFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void processIgnoreTxMessage(DFSIgnoreTx data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void processErrorTxMessage(DFSError data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception;

    public abstract void handleError(MessageObject<String, DFSChangeDelta> message,
                                     Object data,
                                     InvalidTransactionError te) throws Exception;

    public void updateTransaction(long txId,
                                  @NonNull MessageObject<String, DFSChangeDelta> message) throws Exception {
        if (message.mode() == MessageObject.MessageMode.New && txId > 0) {
            stateManager().update(txId);
            LOGGER.debug(getClass(), txId, String.format("Processed transaction delta. [TXID=%d]", txId));
        }
    }

    public boolean checkCloseTxState(@NonNull DFSFileState fileState,
                                     @NonNull MessageObject.MessageMode mode,
                                     long txId) {
        if (mode == MessageObject.MessageMode.Snapshot
                || mode == MessageObject.MessageMode.Backlog) {
            return (txId == fileState.getLastTnxId());
        }
        return txId > fileState.getLastTnxId();
    }

    public DFSTransaction extractTransaction(Object data) {
        if (data instanceof DFSAddFile) {
            return ((DFSAddFile) data).getTransaction();
        } else if (data instanceof DFSAppendFile) {
            return ((DFSAppendFile) data).getTransaction();
        } else if (data instanceof DFSDeleteFile) {
            return ((DFSDeleteFile) data).getTransaction();
        } else if (data instanceof DFSAddBlock) {
            return ((DFSAddBlock) data).getTransaction();
        } else if (data instanceof DFSUpdateBlocks) {
            return ((DFSUpdateBlocks) data).getTransaction();
        } else if (data instanceof DFSTruncateBlock) {
            return ((DFSTruncateBlock) data).getTransaction();
        } else if (data instanceof DFSCloseFile) {
            return ((DFSCloseFile) data).getTransaction();
        } else if (data instanceof DFSRenameFile) {
            return ((DFSRenameFile) data).getTransaction();
        } else if (data instanceof DFSIgnoreTx) {
            return ((DFSIgnoreTx) data).getTransaction();
        } else if (data instanceof DFSError) {
            return ((DFSError) data).getTransaction();
        }
        return null;
    }

    public void processTxMessage(MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        Object data = ChangeDeltaSerDe.parse(message.value());
        DFSTransaction tnx = extractTransaction(data);
        if (tnx != null)
            LOGGER.debug(getClass(), txId,
                    String.format("PROCESSING: [TXID=%d][OP=%s]", tnx.getTransactionId(), tnx.getOp().name()));
        try {
            if (data instanceof DFSAddFile) {
                processAddFileTxMessage((DFSAddFile) data, message, txId);
            } else if (data instanceof DFSAppendFile) {
                processAppendFileTxMessage((DFSAppendFile) data, message, txId);
            } else if (data instanceof DFSDeleteFile) {
                processDeleteFileTxMessage((DFSDeleteFile) data, message, txId);
            } else if (data instanceof DFSAddBlock) {
                processAddBlockTxMessage((DFSAddBlock) data, message, txId);
            } else if (data instanceof DFSUpdateBlocks) {
                processUpdateBlocksTxMessage((DFSUpdateBlocks) data, message, txId);
            } else if (data instanceof DFSTruncateBlock) {
                processTruncateBlockTxMessage((DFSTruncateBlock) data, message, txId);
            } else if (data instanceof DFSCloseFile) {
                processCloseFileTxMessage((DFSCloseFile) data, message, txId);
            } else if (data instanceof DFSRenameFile) {
                processRenameFileTxMessage((DFSRenameFile) data, message, txId);
            } else if (data instanceof DFSIgnoreTx) {
                processIgnoreTxMessage((DFSIgnoreTx) data, message, txId);
            } else if (data instanceof DFSError) {
                processErrorTxMessage((DFSError) data, message, txId);
            } else {
                throw new InvalidMessageError(message.id(), String.format("Message Body type not supported. [type=%s]", data.getClass().getCanonicalName()));
            }
            NameNodeEnv.audit(getClass(), (MessageOrBuilder) data);
        } catch (InvalidTransactionError te) {
            LOGGER.error(getClass(), te.getTxId(), te);
            handleError(message, data, te);
            updateTransaction(txId, message);
            throw new InvalidMessageError(message.id(), te);
        }
    }

    public long checkMessageSequence(MessageObject<String, DFSChangeDelta> message,
                                     boolean ignoreMissing) throws Exception {
        long txId = Long.parseLong(message.value().getTxId());
        if (message.mode() == MessageObject.MessageMode.New) {
            AgentTxState txState = stateManager().agentTxState();
            if (txId != txState.getProcessedTxId() + 1) {
                if (!ignoreMissing) {
                    throw new InvalidMessageError(message.id(),
                            String.format("Detected missing transaction. [expected=%d][current=%d]",
                                    txState.getProcessedTxId() + 1, txId));
                }
            }
            if (txId <= txState.getProcessedTxId()) {
                throw new InvalidMessageError(message.id(),
                        String.format("Duplicate message: Transaction already processed. [TXID=%d][CURRENT=%d]",
                                txId, txState.getProcessedTxId()));
            }
        }
        return txId;
    }
}
