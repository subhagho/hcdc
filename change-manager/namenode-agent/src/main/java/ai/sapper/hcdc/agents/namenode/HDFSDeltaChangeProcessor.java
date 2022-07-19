package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.namenode.model.DFSReplicationState;
import ai.sapper.hcdc.agents.namenode.model.NameNodeTxState;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.messaging.*;
import ai.sapper.hcdc.core.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Getter
@Accessors(fluent = true)
public class HDFSDeltaChangeProcessor implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(HDFSDeltaChangeProcessor.class.getCanonicalName());

    private final ZkStateManager stateManager;
    private HDFSDeltaChangeProcessorConfig processorConfig;
    private MessageSender<String, DFSChangeDelta> sender;
    private MessageSender<String, DFSChangeDelta> errorSender;
    private MessageReceiver<String, DFSChangeDelta> receiver;
    private long receiveBatchTimeout = 1000;

    public HDFSDeltaChangeProcessor(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
    }

    public HDFSDeltaChangeProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            processorConfig = new HDFSDeltaChangeProcessorConfig(xmlConfig);
            processorConfig.read();

            sender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.senderConfig.config())
                    .manager(manger)
                    .connection(processorConfig().senderConfig.connection())
                    .type(processorConfig().senderConfig.type())
                    .partitioner(processorConfig().senderConfig.partitionerClass())
                    .topic(processorConfig().senderConfig.topic())
                    .build();

            receiver = new HCDCMessagingBuilders.ReceiverBuilder()
                    .config(processorConfig().receiverConfig.config())
                    .manager(manger)
                    .connection(processorConfig.receiverConfig.connection())
                    .type(processorConfig.receiverConfig.type())
                    .topic(processorConfig.receiverConfig.topic())
                    .build();

            if (!Strings.isNullOrEmpty(processorConfig.batchTimeout)) {
                receiveBatchTimeout = Long.parseLong(processorConfig.batchTimeout);
            }
            errorSender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.errorConfig.config())
                    .manager(manger)
                    .connection(processorConfig().errorConfig.connection())
                    .type(processorConfig().errorConfig.type())
                    .partitioner(processorConfig().errorConfig.partitionerClass())
                    .topic(processorConfig().errorConfig.topic())
                    .build();

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        Preconditions.checkState(sender != null);
        Preconditions.checkState(receiver != null);
        Preconditions.checkState(errorSender != null);
        try {
            while (NameNodeEnv.get().state().isAvailable()) {
                List<MessageObject<String, DFSChangeDelta>> batch = receiver.nextBatch(receiveBatchTimeout);
                if (batch == null || batch.isEmpty()) {
                    Thread.sleep(receiveBatchTimeout);
                    continue;
                }
                LOG.debug(String.format("Received messages. [count=%d]", batch.size()));
                for (MessageObject<String, DFSChangeDelta> message : batch) {
                    try {
                        long txId = process(message);
                        if (txId > 0) {
                            stateManager.update(txId);
                            LOG.debug(String.format("Processed transaction delta. [TXID=%d]", txId));
                        }
                    } catch (InvalidMessageError ie) {
                        LOG.error("Error processing message.", ie);
                        DefaultLogger.stacktrace(LOG, ie);
                        errorSender.send(message);
                    }
                    receiver.ack(message.id());
                }
            }
            LOG.warn(String.format("Delta Change Processor thread stopped. [env state=%s]", NameNodeEnv.get().state().state().name()));
        } catch (Throwable t) {
            LOG.error("Delta Change Processor terminated with error", t);
            DefaultLogger.stacktrace(LOG, t);
        }
    }


    private long process(MessageObject<String, DFSChangeDelta> message) throws Exception {
        long txId = -1;
        if (!isValidMessage(message)) {
            throw new InvalidMessageError(message.id(),
                    String.format("Invalid Message mode. [id=%s][mode=%s]", message.id(), message.mode().name()));
        }
        txId = checkMessageSequence(message);

        if (message.mode() == MessageObject.MessageMode.Backlog) {
            processBacklogMessage(message, txId);
            txId = -1;
        } else {
            processTxMessage(message, txId);
        }
        return txId;
    }

    private void processTxMessage(MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        Object data = ChangeDeltaSerDe.parse(message.value());
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
            } else {
                throw new InvalidMessageError(message.id(),
                        String.format("Message Body type not supported. [type=%s]", data.getClass().getCanonicalName()));
            }
        } catch (InvalidTransactionError te) {
            processErrorMessage(message, data, te);
            throw new InvalidMessageError(message.id(), te);
        }
    }

    private void processErrorMessage(MessageObject<String, DFSChangeDelta> message, Object data, InvalidTransactionError te) throws Exception {
        if (!Strings.isNullOrEmpty(te.getHdfsPath())) {
            DFSFileState fileState = stateManager.get(te.getHdfsPath());
            if (fileState != null) {
                stateManager.updateState(fileState.getHdfsFilePath(), EFileState.Error);
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

    private DFSTransaction extractTransaction(Object data) {
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
        }
        return null;
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

    private void processAddFileTxMessage(DFSAddFile data,
                                         MessageObject<String, DFSChangeDelta> message,
                                         long txId) throws Exception {
        DFSFileState fileState = stateManager.get(data.getFile().getPath());
        if (fileState != null) {
            if (fileState.getLastTnxId() >= txId) {
                LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                        message.id(), message.mode().name()));
                return;
            } else {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        fileState.getHdfsFilePath(),
                        String.format("Valid File already exists. [path=%s]", fileState.getHdfsFilePath()));
            }
        }
        fileState = stateManager.create(data.getFile().getPath(),
                data.getFile().getInodeId(),
                data.getModifiedTime(),
                data.getBlockSize(),
                EFileState.New,
                data.getTransaction().getTransactionId());
        List<DFSBlock> blocks = data.getBlocksList();
        if (!blocks.isEmpty()) {
            long prevBlockId = -1;
            for (DFSBlock block : blocks) {
                fileState = stateManager.addOrUpdateBlock(fileState.getHdfsFilePath(),
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
        String domain = stateManager.domainManager().matches(fileState.getHdfsFilePath());
        if (!Strings.isNullOrEmpty(domain)) {
            DFSReplicationState rState = stateManager.create(fileState.getId(), fileState.getHdfsFilePath(), true);
            rState.setSnapshotTxId(fileState.getLastTnxId());
            rState.setSnapshotTime(System.currentTimeMillis());
            rState.setSnapshotReady(true);

            stateManager.update(rState);
            sender.send(message);
        } else {
            sendIgnoreTx(message, data);
        }
    }

    private void processAppendFileTxMessage(DFSAppendFile data,
                                            MessageObject<String, DFSChangeDelta> message,
                                            long txId) throws Exception {
        DFSFileState fileState = stateManager.get(data.getFile().getPath());
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
        fileState = stateManager.updateState(fileState.getHdfsFilePath(), EFileState.Updating);

        DFSReplicationState rState = stateManager.get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            DFSFile df = data.getFile();
            df = df.toBuilder().setInodeId(fileState.getId()).build();
            data = data.toBuilder().setFile(df).build();
            message = ChangeDeltaSerDe.create(message.value().getNamespace(), data, DFSAppendFile.class, message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else {
            sendIgnoreTx(message, data);
        }
    }

    private void processDeleteFileTxMessage(DFSDeleteFile data,
                                            MessageObject<String, DFSChangeDelta> message,
                                            long txId) throws Exception {
        DFSFileState fileState = stateManager.get(data.getFile().getPath());
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
        fileState = stateManager.markDeleted(fileState.getHdfsFilePath());

        DFSReplicationState rState = stateManager.get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            stateManager.delete(fileState.getId());

            DFSFile df = data.getFile();
            df = df.toBuilder().setInodeId(fileState.getId()).build();
            data = data.toBuilder().setFile(df).build();

            message = ChangeDeltaSerDe.create(message.value().getNamespace(), data, DFSDeleteFile.class, message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else {
            sendIgnoreTx(message, data);
        }
    }

    private void processAddBlockTxMessage(DFSAddBlock data,
                                          MessageObject<String, DFSChangeDelta> message,
                                          long txId) throws Exception {
        DFSFileState fileState = stateManager.get(data.getFile().getPath());
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
        fileState = stateManager.addOrUpdateBlock(fileState.getHdfsFilePath(),
                data.getLastBlock().getBlockId(),
                lastBlockId,
                data.getTransaction().getTimestamp(),
                data.getLastBlock().getSize(),
                data.getLastBlock().getGenerationStamp(),
                EBlockState.New,
                data.getTransaction().getTransactionId());

        DFSReplicationState rState = stateManager.get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            DFSFile df = data.getFile();
            df = df.toBuilder().setInodeId(fileState.getId()).build();
            data = data.toBuilder().setFile(df).build();

            message = ChangeDeltaSerDe.create(message.value().getNamespace(), data, DFSAddBlock.class, message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else {
            sendIgnoreTx(message, data);
        }
    }

    private void processUpdateBlocksTxMessage(DFSUpdateBlocks data,
                                              MessageObject<String, DFSChangeDelta> message,
                                              long txId) throws Exception {
        DFSFileState fileState = stateManager.get(data.getFile().getPath());
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

            stateManager.updateState(fileState.getHdfsFilePath(), bs.getBlockId(), EBlockState.Updating);
        }
        DFSReplicationState rState = stateManager.get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            DFSFile df = data.getFile();
            df = df.toBuilder().setInodeId(fileState.getId()).build();
            data = data.toBuilder().setFile(df).build();

            message = ChangeDeltaSerDe.create(message.value().getNamespace(), data, DFSUpdateBlocks.class, message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else {
            sendIgnoreTx(message, data);
        }
    }

    private void processTruncateBlockTxMessage(DFSTruncateBlock data,
                                               MessageObject<String, DFSChangeDelta> message,
                                               long txId) throws Exception {
        DFSFileState fileState = stateManager.get(data.getFile().getPath());
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

    private void processCloseFileTxMessage(DFSCloseFile data,
                                           MessageObject<String, DFSChangeDelta> message,
                                           long txId) throws Exception {
        DFSFileState fileState = stateManager.get(data.getFile().getPath());
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
                    fileState = stateManager.addOrUpdateBlock(fileState.getHdfsFilePath(),
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
        fileState = stateManager.updateState(fileState.getHdfsFilePath(), EFileState.Finalized);

        DFSReplicationState rState = stateManager.get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            DFSFile df = data.getFile();
            df = df.toBuilder().setInodeId(fileState.getId()).build();
            DFSCloseFile.Builder builder = data.toBuilder();
            for (int ii = 0; ii < blocks.size(); ii++) {
                builder.removeBlocks(ii);
            }
            builder.setFile(df);
            for (DFSBlock block : blocks) {
                DFSBlock.Builder bb = block.toBuilder();
                DFSBlockState bs = fileState.get(block.getBlockId());

                BlockTnxDelta bd = bs.delta(txId);
                if (bd == null) {
                    throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("Block State out of sync, missing transaction delta. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()));
                }
                bb.setStartOffset(bd.getStartOffset())
                        .setEndOffset(bd.getEndOffset())
                        .setDeltaSize(bd.getEndOffset() - bd.getStartOffset());
                builder.addBlocks(bb.build());
            }
            data = builder.build();

            message = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    data,
                    DFSCloseFile.class,
                    message.mode());
            sender.send(message);
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else {
            sendIgnoreTx(message, data);
        }
    }

    private void processRenameFileTxMessage(DFSRenameFile data,
                                            MessageObject<String, DFSChangeDelta> message,
                                            long txId) throws Exception {
        DFSFileState fileState = stateManager.get(data.getSrcFile().getPath());
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
        fileState = stateManager.markDeleted(data.getSrcFile().getPath());
        EFileState state = (fileState.getState() == EFileState.Error ? fileState.getState() : EFileState.New);
        DFSFileState nfs = stateManager.create(data.getDestFile().getPath(),
                data.getDestFile().getInodeId(),
                fileState.getCreatedTime(),
                fileState.getBlockSize(),
                state,
                txId);
        nfs.setBlocks(fileState.getBlocks());
        nfs.setNumBlocks(fileState.getNumBlocks());
        nfs.setDataSize(fileState.getDataSize());
        nfs.setUpdatedTime(data.getTransaction().getTimestamp());

        nfs = stateManager.update(nfs);

        DFSReplicationState rState = stateManager.get(fileState.getId());
        if (rState != null) {
            stateManager.delete(rState.getInode());
        }
        String domain = stateManager.domainManager().matches(fileState.getHdfsFilePath());
        if (!Strings.isNullOrEmpty(domain)) {
            rState = stateManager.create(nfs.getId(), nfs.getHdfsFilePath(), true);
            rState.setSnapshotTxId(nfs.getLastTnxId());
            rState.setSnapshotTime(System.currentTimeMillis());
            rState.setSnapshotReady(true);

            stateManager.update(rState);
            DFSAddFile addFile = HDFSSnapshotProcessor.generateSnapshot(nfs, true);
            MessageObject<String, DFSChangeDelta> m = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    addFile,
                    DFSAddFile.class,
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

    private void processIgnoreTxMessage(DFSIgnoreTx data,
                                        MessageObject<String, DFSChangeDelta> message,
                                        long txId) throws Exception {
        sender.send(message);
    }

    private void processBacklogMessage(MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSAddFile addFile = (DFSAddFile) ChangeDeltaSerDe.parse(message.value());
        DFSFileState fileState = stateManager.get(addFile.getFile().getPath());
        if (fileState == null) {
            throw new InvalidMessageError(message.id(),
                    String.format("HDFS File Not found. [path=%s]", addFile.getFile().getPath()));
        }
        DFSReplicationState rState = stateManager.get(fileState.getId());
        if (rState == null || !rState.isEnabled()) {
            throw new InvalidMessageError(message.id(),
                    String.format("HDFS File not registered for snapshot. [path=%s][inode=%d]",
                            addFile.getFile().getPath(), fileState.getId()));
        }
        if (rState.isSnapshotReady()) {
            throw new InvalidMessageError(message.id(),
                    String.format("Snapshot already completed for file. [path=%s][inode=%d]",
                            addFile.getFile().getPath(), fileState.getId()));
        }
        if (rState.getSnapshotTxId() != txId) {
            throw new InvalidMessageError(message.id(),
                    String.format("Snapshot transaction mismatch. [path=%s][inode=%d] [expected=%d][actual=%d]",
                            addFile.getFile().getPath(), fileState.getId(), rState.getSnapshotTxId(), txId));
        }

    }

    private long checkMessageSequence(MessageObject<String, DFSChangeDelta> message) throws Exception {
        long txId = Long.parseLong(message.value().getTxId());
        if (message.mode() == MessageObject.MessageMode.New) {
            NameNodeTxState txState = stateManager.agentTxState();
            if (txState.getProcessedTxId() + 1 != txId) {
                throw new Exception(String.format("Detected missing transaction. [expected TX ID=%d][actual TX ID=%d]",
                        (txState.getProcessedTxId() + 1), txId));
            }
        }
        return txId;
    }

    private boolean isValidMessage(MessageObject<String, DFSChangeDelta> message) {
        boolean ret = false;
        if (message.mode() != null) {
            ret = (message.mode() == MessageObject.MessageMode.New || message.mode() == MessageObject.MessageMode.Backlog);
        }
        if (ret) {
            ret = message.value().hasTxId();
        }
        return ret;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class HDFSDeltaChangeProcessorConfig extends ConfigReader {
        public static class Constants {
            public static final String __CONFIG_PATH = "delta.manager";
            public static final String __CONFIG_PATH_SENDER = "sender";
            public static final String __CONFIG_PATH_RECEIVER = "receiver";
            public static final String __CONFIG_PATH_ERROR = "errorQueue";
            public static final String CONFIG_RECEIVE_TIMEOUT = "timeout";
        }

        private MessagingConfig senderConfig;
        private MessagingConfig receiverConfig;
        private MessagingConfig errorConfig;
        private String batchTimeout;

        public HDFSDeltaChangeProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Constants.__CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                HierarchicalConfiguration<ImmutableNode> config = get().configurationAt(Constants.__CONFIG_PATH_SENDER);
                if (config == null) {
                    throw new ConfigurationException(String.format("Sender configuration node not found. [path=%s]", Constants.__CONFIG_PATH_SENDER));
                }
                senderConfig = new MessagingConfig();
                senderConfig.read(config);
                if (config.containsKey(Constants.CONFIG_RECEIVE_TIMEOUT)) {
                    batchTimeout = config.getString(Constants.CONFIG_RECEIVE_TIMEOUT);
                }

                config = get().configurationAt(Constants.__CONFIG_PATH_RECEIVER);
                if (config == null) {
                    throw new ConfigurationException(String.format("Receiver configuration node not found. [path=%s]", Constants.__CONFIG_PATH_RECEIVER));
                }
                receiverConfig = new MessagingConfig();
                receiverConfig.read(config);

                config = get().configurationAt(Constants.__CONFIG_PATH_ERROR);
                if (config == null) {
                    throw new ConfigurationException(String.format("Error Queue configuration node not found. [path=%s]", Constants.__CONFIG_PATH_ERROR));
                }
                errorConfig = new MessagingConfig();
                errorConfig.read(config);

            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
