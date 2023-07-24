/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.InvalidTransactionError;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.model.*;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.core.model.dfs.EBlockState;
import ai.sapper.cdc.core.processing.EventProcessorMetrics;
import ai.sapper.cdc.core.processing.ProcessingState;
import ai.sapper.cdc.core.state.HCdcStateManager;
import ai.sapper.cdc.core.utils.ProtoUtils;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import ai.sapper.cdc.entity.schema.SchemaEntity;
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
    private final HCdcStateManager stateManager;
    private final HCdcSchemaManager schemaManager;
    private MessageSender<String, DFSChangeDelta> errorSender;
    private final NameNodeEnv env;
    private final HCdcBaseMetrics metrics;

    public TransactionProcessor(@NonNull String name,
                                @NonNull NameNodeEnv env,
                                @NonNull HCdcBaseMetrics metrics) {
        this.name = name;
        this.env = env;
        this.stateManager = env.stateManager();
        this.schemaManager = env.schemaManager();
        this.metrics = metrics;
    }

    public TransactionProcessor withErrorQueue(@NonNull MessageSender<String, DFSChangeDelta> errorSender) {
        this.errorSender = errorSender;
        return this;
    }


    public abstract void processAddFileTxMessage(@NonNull DFSFileAdd data,
                                                 @NonNull MessageObject<String, DFSChangeDelta> message,
                                                 @NonNull Params params) throws Exception;

    public SchemaEntity isRegistered(String hdfsPath) throws Exception {
        Preconditions.checkNotNull(schemaManager);
        return schemaManager.matches(hdfsPath);
    }

    public abstract void processAppendFileTxMessage(@NonNull DFSFileAppend data,
                                                    @NonNull MessageObject<String, DFSChangeDelta> message,
                                                    @NonNull Params params) throws Exception;

    public abstract void processDeleteFileTxMessage(@NonNull DFSFileDelete data,
                                                    @NonNull MessageObject<String, DFSChangeDelta> message,
                                                    @NonNull Params params) throws Exception;

    public abstract void processAddBlockTxMessage(@NonNull DFSBlockAdd data,
                                                  @NonNull MessageObject<String, DFSChangeDelta> message,
                                                  @NonNull Params params) throws Exception;

    public abstract void processUpdateBlocksTxMessage(@NonNull DFSBlockUpdate data,
                                                      @NonNull MessageObject<String, DFSChangeDelta> message,
                                                      @NonNull Params params) throws Exception;

    public abstract void processTruncateBlockTxMessage(@NonNull DFSBlockTruncate data,
                                                       @NonNull MessageObject<String, DFSChangeDelta> message,
                                                       @NonNull Params params) throws Exception;

    public abstract void processCloseFileTxMessage(@NonNull DFSFileClose data,
                                                   @NonNull MessageObject<String, DFSChangeDelta> message,
                                                   @NonNull Params params) throws Exception;

    public abstract void processRenameFileTxMessage(@NonNull DFSFileRename data,
                                                    @NonNull MessageObject<String, DFSChangeDelta> message,
                                                    @NonNull Params params) throws Exception;

    public abstract void processIgnoreTxMessage(@NonNull DFSIgnoreTx data,
                                                @NonNull MessageObject<String, DFSChangeDelta> message,
                                                @NonNull Params params) throws Exception;

    public abstract void processErrorTxMessage(@NonNull DFSError data,
                                               @NonNull MessageObject<String, DFSChangeDelta> message,
                                               @NonNull Params params) throws Exception;

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
                                         @NonNull Params params) throws InvalidTransactionError {
        if (fileState == null || fileState.checkDeleted()) {
            throw new InvalidTransactionError(params.txId().getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    file.getEntity().getEntity(),
                    new Exception(String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            file.getEntity().getEntity())))
                    .withFile(file);
        }
        if (message.mode() == MessageObject.MessageMode.New) {
            if (fileState.getLastTnxId() >= params.txId().getId() && !params.retry()) {
                LOGGER.warn(getClass(), params.txId().getId(),
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
                                 @NonNull Params params) throws Exception {
        if (data instanceof DFSFileAdd) {
            metrics.metricsEventAddFile().increment();
            processAddFileTxMessage((DFSFileAdd) data, message, params);
        } else if (data instanceof DFSFileAppend) {
            metrics.metricsEventAppendFile().increment();
            processAppendFileTxMessage((DFSFileAppend) data, message, params);
        } else if (data instanceof DFSFileDelete) {
            metrics.metricsEventDeleteFile().increment();
            processDeleteFileTxMessage((DFSFileDelete) data, message, params);
        } else if (data instanceof DFSBlockAdd) {
            metrics.metricsEventAddBlock().increment();
            processAddBlockTxMessage((DFSBlockAdd) data, message, params);
        } else if (data instanceof DFSBlockUpdate) {
            metrics.metricsEventUpdateBlock().increment();
            processUpdateBlocksTxMessage((DFSBlockUpdate) data, message, params);
        } else if (data instanceof DFSBlockTruncate) {
            metrics.metricsEventTruncateBlock().increment();
            processTruncateBlockTxMessage((DFSBlockTruncate) data, message, params);
        } else if (data instanceof DFSFileClose) {
            metrics.metricsEventCloseFile().increment();
            processCloseFileTxMessage((DFSFileClose) data, message, params);
        } else if (data instanceof DFSFileRename) {
            metrics.metricsEventRenameFile().increment();
            processRenameFileTxMessage((DFSFileRename) data, message, params);
        } else if (data instanceof DFSIgnoreTx) {
            metrics.metricsEventIgnore().increment();
            processIgnoreTxMessage((DFSIgnoreTx) data, message, params);
        } else if (data instanceof DFSError) {
            metrics.metricsEventError().increment();
            processErrorTxMessage((DFSError) data, message, params);
        } else {
            throw new InvalidMessageError(message.id(), String.format("Message Body type not supported. [type=%s]", data.getClass().getCanonicalName()));
        }
    }

    public HCdcTxId checkMessageSequence(MessageObject<String, DFSChangeDelta> message,
                                         boolean ignoreMissing,
                                         boolean retry) throws Exception {
        HCdcTxId txId = ProtoUtils.fromTx(message.value().getTx());
        if (message.mode() == MessageObject.MessageMode.New) {
            ProcessingState<EHCdcProcessorState, HCdcTxId> txState = stateManager().processingState();
            long offset = txState.getOffset().getId();
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
            if (txId.compare(txState.getOffset()) <= 0) {
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
                                        @NonNull Params params,
                                        long updateTime,
                                        long blockSize,
                                        List<DFSBlock> blocks,
                                        @NonNull EFileState fState,
                                        @NonNull EBlockState bState) throws Exception {
        String path = file.getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState != null) {
            if (!params.retry()) {
                if (fileState.getLastTnxId() >= params.txId().getId() &&
                        message.mode() == MessageObject.MessageMode.New) {
                    LOGGER.warn(getClass(), params.txId().getId(),
                            String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                                    message.id(), message.mode().name()));
                    return null;
                } else if (!fileState.checkDeleted()) {
                    throw new InvalidTransactionError(params.txId().getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(String.format("Valid File already exists. [path=%s]",
                                    fileState.getFileInfo().getHdfsPath())))
                            .withFile(file);
                }
            } else if (fileState.getLastTnxId() != params.txId().getId()) {
                throw new InvalidTransactionError(params.txId().getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(
                                String.format("Invalid processed transaction. [path=%s][expected tx=%d][processed tx=%d]",
                                        fileState.getFileInfo().getHdfsPath(), params.txId().getId(), fileState.getLastTnxId())))
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
                        params.txId().getId());
        if (ProtoBufUtils.update(fileState, file)) {
            fileState = stateManager()
                    .fileStateHelper()
                    .update(fileState);
        }
        if (!blocks.isEmpty()) {
            long prevBlockId = -1;
            for (DFSBlock block : blocks) {
                DFSBlockState bs = null;
                if (params.retry()) {
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
                                    params.txId().getId());
                prevBlockId = block.getBlockId();
            }
        }
        fileState.setState(fState);

        return fileState;
    }
}
