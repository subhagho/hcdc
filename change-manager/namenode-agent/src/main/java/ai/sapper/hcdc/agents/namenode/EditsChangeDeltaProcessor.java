package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.filters.DomainManager;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.model.AgentTxState;
import ai.sapper.cdc.core.model.BlockTransactionDelta;
import ai.sapper.cdc.core.utils.SchemaEntityHelper;
import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ProcessorStateManager;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import ai.sapper.hcdc.agents.model.DFSTransactionType;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSFileClose;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public class EditsChangeDeltaProcessor extends ChangeDeltaProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(EditsChangeDeltaProcessor.class.getCanonicalName());

    private EditsChangeTransactionProcessor processor;

    private long receiveBatchTimeout = 1000;

    public EditsChangeDeltaProcessor(@NonNull ZkStateManager stateManager, @NonNull String name) {
        super(stateManager, name);
    }

    public EditsChangeDeltaProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                          @NonNull ConnectionManager manger) throws ConfigurationException {
        ChangeDeltaProcessorConfig config = new ChangeDeltaProcessorConfig(xmlConfig);
        super.init(config, manger);
        processor = (EditsChangeTransactionProcessor) new EditsChangeTransactionProcessor(name())
                .withSenderQueue(sender())
                .withStateManager(stateManager())
                .withErrorQueue(errorSender());
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
    public void doRun() throws Exception {
        Preconditions.checkState(sender() != null);
        Preconditions.checkState(receiver() != null);
        Preconditions.checkState(errorSender() != null);
        Preconditions.checkState(stateManager() instanceof ProcessorStateManager);
        try {
            while (NameNodeEnv.get(name()).state().isAvailable()) {
                List<MessageObject<String, DFSChangeDelta>> batch = receiver().nextBatch(receiveBatchTimeout);
                if (batch == null || batch.isEmpty()) {
                    Thread.sleep(receiveBatchTimeout);
                    continue;
                }
                DomainManager dm = ((ProcessorStateManager) stateManager()).domainManager();
                dm.refresh();

                LOG.debug(String.format("Received messages. [count=%d]", batch.size()));
                for (MessageObject<String, DFSChangeDelta> message : batch) {
                    long txId = -1;
                    stateManager().stateLock();
                    try {
                        try {
                            txId = process(message);
                            LOGGER.info(getClass(), txId, "Transaction read successfully...");
                        } catch (InvalidMessageError ie) {
                            LOG.error("Error processing message.", ie);
                            DefaultLogger.stacktrace(LOG, ie);
                            errorSender().send(message);
                        }
                        processor.updateTransaction(txId, message);
                        receiver().ack(message.id());
                    } finally {
                        stateManager().stateUnlock();
                    }
                }
            }
            LOG.warn("Delta Change Processor thread stopped.");
        } catch (Throwable t) {
            LOG.error("Delta Change Processor terminated with error", t);
            DefaultLogger.stacktrace(LOG, t);
            throw t;
        }
    }


    private long process(MessageObject<String, DFSChangeDelta> message) throws Exception {
        long txId = -1;
        if (!isValidMessage(message)) {
            throw new InvalidMessageError(message.id(),
                    String.format("Invalid Message mode. [id=%s][mode=%s]", message.id(), message.mode().name()));
        }
        AgentTxState state = updateReadState(message.id());
        boolean retry = false;
        if (!Strings.isNullOrEmpty(state.getCurrentMessageId()) &&
                !Strings.isNullOrEmpty(lastMessageId()))
            retry = state.getCurrentMessageId().compareTo(lastMessageId()) == 0;
        txId = processor.checkMessageSequence(message, false, retry);
        if (message.mode() == MessageObject.MessageMode.Backlog) {
            processBacklogMessage(message, txId);
            txId = -1;
        } else {
            processor.processTxMessage(message, txId, retry);
        }
        return txId;
    }

    private void processBacklogMessage(MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileClose closeFile = ChangeDeltaSerDe.parse(message.value(), DFSFileClose.class);
        DFSFileState fileState = stateManager()
                .fileStateHelper()
                .get(closeFile.getFile().getEntity().getEntity());
        if (fileState == null) {
            throw new InvalidMessageError(message.id(),
                    String.format("HDFS File Not found. [path=%s]", closeFile.getFile().getEntity().getEntity()));
        }
        SchemaEntity schemaEntity = processor.isRegistered(fileState.getFileInfo().getHdfsPath());
        if (schemaEntity == null) {
            throw new InvalidMessageError(message.id(),
                    String.format("HDFS File Not registered. [path=%s]",
                            closeFile.getFile().getEntity().getEntity()));
        }
        DFSFileReplicaState rState = stateManager()
                .replicaStateHelper()
                .get(schemaEntity, fileState.getFileInfo().getInodeId());
        if (rState == null || !rState.isEnabled()) {
            throw new InvalidMessageError(message.id(),
                    String.format("HDFS File not registered for snapshot. [path=%s][inode=%d]",
                            closeFile.getFile().getEntity().getEntity(),
                            fileState.getFileInfo().getInodeId()));
        }
        if (rState.isSnapshotReady()) {
            throw new InvalidMessageError(message.id(),
                    String.format("Snapshot already completed for file. [path=%s][inode=%d]",
                            closeFile.getFile().getEntity().getEntity(),
                            fileState.getFileInfo().getInodeId()));
        }
        if (rState.getSnapshotTxId() != txId) {
            throw new InvalidMessageError(message.id(),
                    String.format("Snapshot transaction mismatch. [path=%s][inode=%d] [expected=%d][actual=%d]",
                            closeFile.getFile().getEntity().getEntity(),
                            fileState.getFileInfo().getInodeId(),
                            rState.getSnapshotTxId(), txId));
        }
        if (fileState.getLastTnxId() > txId)
            sendBackLogMessage(message, fileState, rState, txId);
    }

    private void sendBackLogMessage(MessageObject<String, DFSChangeDelta> message,
                                    DFSFileState fileState,
                                    DFSFileReplicaState rState,
                                    long txId) throws Exception {
        DFSTransactionType.DFSCloseFileType tnx = buildBacklogTransactions(fileState, rState, txId + 1);
        if (tnx != null) {
            SchemaEntity schemaEntity = SchemaEntityHelper.parse(message.value().getEntity());

            DFSFileClose closeFile = tnx.convertToProto();
            MessageObject<String, DFSChangeDelta> mesg = ChangeDeltaSerDe.create(closeFile,
                    DFSFileClose.class,
                    schemaEntity,
                    message.value().getSequence(),
                    MessageObject.MessageMode.Backlog);
            sender().send(mesg);

        }
    }

    private DFSTransactionType.DFSCloseFileType buildBacklogTransactions(DFSFileState fileState,
                                                                         DFSFileReplicaState rState,
                                                                         long txId) throws Exception {
        DFSTransactionType.DFSFileType file = new DFSTransactionType.DFSFileType();
        file.namespace(fileState.getFileInfo().getNamespace())
                .path(fileState.getFileInfo().getHdfsPath())
                .inodeId(fileState.getFileInfo().getInodeId());

        DFSTransactionType.DFSCloseFileType closeFile = new DFSTransactionType.DFSCloseFileType();
        closeFile.file(file)
                .overwrite(false)
                .blockSize(fileState.getBlockSize())
                .modifiedTime(fileState.getUpdatedTime())
                .accessedTime(fileState.getCreatedTime())
                .length(fileState.getDataSize());

        if (fileState.hasBlocks()) {
            for (DFSBlockState bs : fileState.getBlocks()) {
                BlockTransactionDelta delta = bs.compressedChangeSet(txId);
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

    private boolean isValidMessage(MessageObject<String, DFSChangeDelta> message) {
        boolean ret = false;
        if (message.mode() != null) {
            ret = (message.mode() == MessageObject.MessageMode.New
                    || message.mode() == MessageObject.MessageMode.Backlog);
        }
        if (ret) {
            ret = message.value().hasTxId();
        }
        return ret;
    }

    @Override
    public void close() throws IOException {

    }
}
