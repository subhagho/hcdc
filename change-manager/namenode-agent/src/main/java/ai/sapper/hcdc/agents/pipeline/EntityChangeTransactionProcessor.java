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

package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.core.InvalidTransactionError;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.model.EFileState;
import ai.sapper.cdc.core.model.Params;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.core.model.dfs.EBlockState;
import ai.sapper.hcdc.agents.common.ProtoBufUtils;
import ai.sapper.hcdc.agents.common.TransactionProcessor;
import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Strings;
import lombok.NonNull;

import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

public class EntityChangeTransactionProcessor extends TransactionProcessor {
    private MessageSender<String, DFSChangeDelta> sender;

    public EntityChangeTransactionProcessor(@NonNull String name, @NonNull NameNodeEnv env) {
        super(name, env);
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
     * @throws Exception
     */
    @Override
    public void processAddFileTxMessage(@NonNull DFSFileAdd data,
                                        @NonNull MessageObject<String, DFSChangeDelta> message,
                                        @NonNull Params params) throws Exception {
        DFSFileState fileState = createFileState(data.getFile(),
                message,
                params,
                data.getModifiedTime(),
                data.getBlockSize(),
                data.getBlocksList(),
                EFileState.Updating,
                EBlockState.Updating);
        if (fileState == null) return;

        sender.send(message);
        fileState = stateManager()
                .fileStateHelper()
                .update(fileState);
    }

    /**
     * @param data
     * @param message
     * @throws Exception
     */
    @Override
    public void processAppendFileTxMessage(@NonNull DFSFileAppend data,
                                           @NonNull MessageObject<String, DFSChangeDelta> message,
                                           @NonNull Params params) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (!checkTransactionState(fileState, data.getFile(), message, params)) {
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
     * @throws Exception
     */
    @Override
    public void processDeleteFileTxMessage(@NonNull DFSFileDelete data,
                                           @NonNull MessageObject<String, DFSChangeDelta> message,
                                           @NonNull Params params) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (fileState == null) {
            if (params.retry())
                return;
        }
        if (!checkTransactionState(fileState, data.getFile(), message, params)) {
            return;
        }

        sender.send(message);

        if (fileState != null) {
            fileState = stateManager()
                    .fileStateHelper()
                    .markDeleted(fileState.getFileInfo().getHdfsPath(), false);
        }
    }

    /**
     * @param data
     * @param message
     * @throws Exception
     */
    @Override
    public void processAddBlockTxMessage(@NonNull DFSBlockAdd data,
                                         @NonNull MessageObject<String, DFSChangeDelta> message,
                                         @NonNull Params params) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (!checkTransactionState(fileState, data.getFile(), message, params)) {
            return;
        }
        long lastBlockId = -1;
        if (data.hasPenultimateBlock()) {
            lastBlockId = data.getPenultimateBlock().getBlockId();
        }
        DFSBlockState blockState = null;
        if (params.retry()) {
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
        sender.send(message);
    }

    /**
     * @param data
     * @param message
     * @throws Exception
     */
    @Override
    public void processUpdateBlocksTxMessage(@NonNull DFSBlockUpdate data,
                                             @NonNull MessageObject<String, DFSChangeDelta> message,
                                             @NonNull Params params) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (!checkTransactionState(fileState, data.getFile(), message, params)) {
            return;
        }
        List<DFSBlock> blocks = data.getBlocksList();
        if (blocks.isEmpty()) {
            throw new InvalidTransactionError(params.txId().getId(),
                    DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getFileInfo().getHdfsPath(),
                    new Exception(String.format("File State out of sync, no block to update. [path=%s]",
                            fileState.getFileInfo().getHdfsPath())))
                    .withFile(data.getFile());
        }
        for (DFSBlock block : blocks) {
            DFSBlockState bs = fileState.get(block.getBlockId());
            if (bs == null) {
                throw new InvalidTransactionError(params.txId().getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                        .withFile(data.getFile());
            } else if (bs.getDataSize() != block.getSize()) {
                throw new InvalidTransactionError(params.txId().getId(),
                        DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getFileInfo().getHdfsPath(),
                        new Exception(String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
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
     * @throws Exception
     */
    @Override
    public void processTruncateBlockTxMessage(@NonNull DFSBlockTruncate data,
                                              @NonNull MessageObject<String, DFSChangeDelta> message,
                                              @NonNull Params params) throws Exception {
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (!checkTransactionState(fileState, data.getFile(), message, params)) {
            return;
        }
    }


    /**
     * @param data
     * @param message
     * @throws Exception
     */
    @Override
    public void processCloseFileTxMessage(@NonNull DFSFileClose data,
                                          @NonNull MessageObject<String, DFSChangeDelta> message,
                                          @NonNull Params params) throws Exception {
        if (message.mode() == MessageObject.MessageMode.Snapshot) {
            DFSFileState fileState = createFileState(data.getFile(),
                    message,
                    params,
                    data.getModifiedTime(),
                    data.getBlockSize(),
                    data.getBlocksList(),
                    EFileState.New,
                    EBlockState.New);
            if (fileState == null) return;
        }
        String path = data.getFile().getEntity().getEntity();
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(path);
        if (!checkTransactionState(fileState, data.getFile(), message, params)) {
            return;
        }

        List<DFSBlock> blocks = data.getBlocksList();
        if (!blocks.isEmpty()) {
            for (DFSBlock block : blocks) {
                DFSBlockState bs = fileState.get(block.getBlockId());
                if (bs == null) {
                    throw new InvalidTransactionError(params.txId().getId(),
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
                                    params.txId().getId());
                } else if (bs.getDataSize() != block.getSize()) {
                    throw new InvalidTransactionError(params.txId().getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(String.format("File State out of sync, block size mismatch. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
                            .withFile(data.getFile());
                } else {
                    throw new InvalidTransactionError(params.txId().getId(),
                            DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getFileInfo().getHdfsPath(),
                            new Exception(String.format("File State out of sync, block state mismatch. [path=%s][blockID=%d]",
                                    fileState.getFileInfo().getHdfsPath(), block.getBlockId())))
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
     * @throws Exception
     */
    @Override
    public void processRenameFileTxMessage(@NonNull DFSFileRename data,
                                           @NonNull MessageObject<String, DFSChangeDelta> message,
                                           @NonNull Params params) throws Exception {
        throw new InvalidMessageError(message.id(), "Rename transaction should not come...");
    }

    /**
     * @param data
     * @param message
     * @throws Exception
     */
    @Override
    public void processIgnoreTxMessage(@NonNull DFSIgnoreTx data,
                                       @NonNull MessageObject<String, DFSChangeDelta> message,
                                       @NonNull Params params) throws Exception {
        LOGGER.debug(getClass(),
                params.txId().getId(),
                String.format("Received Ignore Transaction: [ID=%d]", params.txId().getId()));
    }

    /**
     * @param data
     * @param message
     * @throws Exception
     */
    @Override
    public void processErrorTxMessage(@NonNull DFSError data,
                                      @NonNull MessageObject<String, DFSChangeDelta> message,
                                      @NonNull Params params) throws Exception {
        DFSTransaction tnx = extractTransaction(data);
        if (data.hasFile()) {
            DFSFile df = data.getFile();
            DFSFileState fileState = stateManager()
                    .fileStateHelper()
                    .get(df.getEntity().getEntity());
            if (fileState != null) {
                fileState.setState(EFileState.Error);
                if (tnx != null) {
                    fileState.setLastTnxId(tnx.getId());
                }
                fileState.setTimestamp(System.currentTimeMillis());
                stateManager().fileStateHelper().update(fileState);
            }
        }
        LOGGER.warn(getClass(), params.txId().getId(),
                String.format("Received Error Message: %s. [TX=%d][ERROR CODE=%s]",
                        data.getMessage(), params.txId().getId(), data.getCode().name()));
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
                            @NonNull InvalidTransactionError te) throws Exception {
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
    }
}
