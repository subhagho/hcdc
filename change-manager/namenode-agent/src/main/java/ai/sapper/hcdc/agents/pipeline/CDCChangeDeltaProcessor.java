package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.namenode.model.DFSReplicationState;
import ai.sapper.hcdc.agents.namenode.model.DFSTransactionType;
import ai.sapper.hcdc.agents.namenode.model.NameNodeTxState;
import ai.sapper.hcdc.common.model.DFSAddFile;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSCloseFile;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.hcdc.core.messaging.InvalidMessageError;
import ai.sapper.hcdc.core.messaging.MessageObject;
import ai.sapper.hcdc.core.model.BlockTnxDelta;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Getter
@Accessors(fluent = true)
public class CDCChangeDeltaProcessor extends ChangeDeltaProcessor {
    private static Logger LOG = LoggerFactory.getLogger(CDCChangeDeltaProcessor.class.getCanonicalName());

    private CDCTransactionProcessor processor;

    private long receiveBatchTimeout = 1000;

    public CDCChangeDeltaProcessor(@NonNull ZkStateManager stateManager) {
        super(stateManager);
    }

    public CDCChangeDeltaProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                        @NonNull ConnectionManager manger) throws ConfigurationException {
        ChangeDeltaProcessorConfig config = new CDCChangeDeltaProcessorConfig(xmlConfig);
        super.init(config, manger);
        processor = (CDCTransactionProcessor) new CDCTransactionProcessor()
                .withSenderQueue(sender());
        return this;
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
        Preconditions.checkState(sender() != null);
        Preconditions.checkState(receiver() != null);
        Preconditions.checkState(errorSender() != null);
        try {
            while (NameNodeEnv.get().state().isAvailable()) {
                List<MessageObject<String, DFSChangeDelta>> batch = receiver().nextBatch(receiveBatchTimeout);
                if (batch == null || batch.isEmpty()) {
                    Thread.sleep(receiveBatchTimeout);
                    continue;
                }
                LOG.debug(String.format("Received messages. [count=%d]", batch.size()));
                for (MessageObject<String, DFSChangeDelta> message : batch) {
                    stateManager().replicationLock().lock();
                    try {
                        try {
                            long txId = process(message);
                            if (txId > 0) {
                                stateManager().update(txId);
                                LOG.debug(String.format("Processed transaction delta. [TXID=%d]", txId));
                            }
                        } catch (InvalidMessageError ie) {
                            LOG.error("Error processing message.", ie);
                            DefaultLogger.stacktrace(LOG, ie);
                            errorSender().send(message);
                        }
                        receiver().ack(message.id());
                    } finally {
                        stateManager().replicationLock().unlock();
                    }
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
        } else if (message.mode() == MessageObject.MessageMode.Snapshot) {

        } else {
            processor.processTxMessage(message, txId);
        }
        return txId;
    }

    private void processBacklogMessage(MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSAddFile addFile = (DFSAddFile) ChangeDeltaSerDe.parse(message.value());
        DFSFileState fileState = stateManager().get(addFile.getFile().getPath());
        if (fileState == null) {
            throw new InvalidMessageError(message.id(),
                    String.format("HDFS File Not found. [path=%s]", addFile.getFile().getPath()));
        }
        DFSReplicationState rState = stateManager().get(fileState.getId());
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
        if (fileState.getLastTnxId() > txId)
            sendBackLogMessage(message, fileState, rState, txId);
    }

    private void sendBackLogMessage(MessageObject<String, DFSChangeDelta> message,
                                    DFSFileState fileState,
                                    DFSReplicationState rState,
                                    long txId) throws Exception {
        DFSTransactionType.DFSCloseFileType tnx = buildBacklogTransactions(fileState, rState, txId);
        if (tnx != null) {
            DFSCloseFile closeFile = tnx.convertToProto();
            MessageObject<String, DFSChangeDelta> mesg = ChangeDeltaSerDe.create(message.value().getNamespace(),
                    closeFile,
                    DFSCloseFile.class,
                    rState.getEntity().getDomain(),
                    rState.getEntity().getEntity(),
                    MessageObject.MessageMode.Backlog);
            sender().send(mesg);
            rState = stateManager().update(rState);
        }
    }

    private DFSTransactionType.DFSCloseFileType buildBacklogTransactions(DFSFileState fileState,
                                                                         DFSReplicationState rState,
                                                                         long txId) throws Exception {
        DFSTransactionType.DFSFileType file = new DFSTransactionType.DFSFileType();
        file.path(fileState.getHdfsFilePath());
        file.inodeId(fileState.getId());

        DFSTransactionType.DFSCloseFileType closeFile = new DFSTransactionType.DFSCloseFileType();
        closeFile.file(file);
        closeFile.overwrite(false);
        closeFile.blockSize(fileState.getBlockSize());
        closeFile.modifiedTime(fileState.getUpdatedTime());
        closeFile.accessedTime(fileState.getCreatedTime());
        closeFile.length(fileState.getDataSize());

        if (fileState.hasBlocks()) {
            for (DFSBlockState bs : fileState.getBlocks()) {
                BlockTnxDelta delta = bs.compressedChangeSet(txId);
                if (delta != null) {
                    DFSTransactionType.DFSBlockType bd = new DFSTransactionType.DFSBlockType();
                    bd.blockId(bs.getBlockId());
                    bd.generationStamp(bs.getGenerationStamp());
                    bd.startOffset(delta.getStartOffset());
                    bd.endOffset(delta.getEndOffset());
                    bd.deltaSize(bd.endOffset() - bd.startOffset() + 1);
                    closeFile.addBlock(bd);
                }
            }
        }
        rState.setLastReplicatedTx(fileState.getLastTnxId());
        rState.setLastReplicationTime(System.currentTimeMillis());

        return closeFile;
    }

    private long checkMessageSequence(MessageObject<String, DFSChangeDelta> message) throws Exception {
        long txId = Long.parseLong(message.value().getTxId());
        if (message.mode() == MessageObject.MessageMode.New) {
            NameNodeTxState txState = stateManager().agentTxState();
            if (txState.getProcessedTxId() + 1 != txId) {
                if (txId <= txState.getProcessedTxId()) {
                    throw new InvalidMessageError(message.id(),
                            String.format("Duplicate message: Transaction already processed. [TXID=%d][CURRENT=%d]",
                                    txId, txState.getProcessedTxId()));
                } else {
                    throw new Exception(String.format("Detected missing transaction. [expected TX ID=%d][actual TX ID=%d]",
                            (txState.getProcessedTxId() + 1), txId));
                }
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

    public static class CDCChangeDeltaProcessorConfig extends ChangeDeltaProcessorConfig {
        public static final String __CONFIG_PATH = "processor.cdc";

        public CDCChangeDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }
    }
}
