package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.filters.DomainManager;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.model.BlockTransactionDelta;
import ai.sapper.cdc.core.utils.SchemaEntityHelper;
import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.common.ProcessorStateManager;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import ai.sapper.hcdc.agents.model.DFSTransactionType;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSFileClose;
import ai.sapper.hcdc.common.model.DFSTransaction;
import ai.sapper.cdc.core.utils.ProtoUtils;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class EditsChangeDeltaProcessor extends ChangeDeltaProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(EditsChangeDeltaProcessor.class.getCanonicalName());

    public EditsChangeDeltaProcessor(@NonNull ZkStateManager stateManager,
                                     @NonNull String name) {
        super(stateManager, name, EProcessorMode.Committer, false);
    }

    @Override
    public boolean isValidMessage(@NonNull MessageObject<String, DFSChangeDelta> message) {
        boolean ret = false;
        if (message.mode() != null) {
            ret = (message.mode() == MessageObject.MessageMode.New
                    || message.mode() == MessageObject.MessageMode.Backlog);
        }
        if (ret) {
            ret = message.value().hasTx();
        }
        return ret;
    }


    public ChangeDeltaProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                     @NonNull ConnectionManager manger) throws ConfigurationException {
        ChangeDeltaProcessorConfig config = new ChangeDeltaProcessorConfig(xmlConfig);
        super.init(config, manger);
        EditsChangeTransactionProcessor processor = (EditsChangeTransactionProcessor) new EditsChangeTransactionProcessor(name())
                .withSenderQueue(sender())
                .withStateManager(stateManager())
                .withErrorQueue(errorSender());
        return withProcessor(processor);
    }

    @Override
    public void batchStart() throws Exception {
        Preconditions.checkState(stateManager() instanceof ProcessorStateManager);
        DomainManager dm = ((ProcessorStateManager) stateManager()).domainManager();
        dm.refresh();
    }

    @Override
    public void batchEnd() throws Exception {

    }

    @Override
    public void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                        @NonNull Object data,
                        DFSTransaction tnx,
                        boolean retry) throws Exception {
        BaseTxId txId = null;
        if (tnx != null) {
            txId = ProtoUtils.fromTx(tnx);
        } else {
            txId = new BaseTxId(-1);
        }
        EditsChangeTransactionProcessor processor
                = (EditsChangeTransactionProcessor) processor();
        if (message.mode() == MessageObject.MessageMode.Backlog) {
            processBacklogMessage(message, txId);
        } else {
            processor.processTxMessage(message, data, txId, retry);
        }
    }

    private void processBacklogMessage(MessageObject<String, DFSChangeDelta> message, BaseTxId txId) throws Exception {
        EditsChangeTransactionProcessor processor
                = (EditsChangeTransactionProcessor) processor();
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
        if (rState.getSnapshotTxId() != txId.getId()) {
            throw new InvalidMessageError(message.id(),
                    String.format("Snapshot transaction mismatch. [path=%s][inode=%d] [expected=%d][actual=%d]",
                            closeFile.getFile().getEntity().getEntity(),
                            fileState.getFileInfo().getInodeId(),
                            rState.getSnapshotTxId(), txId.getId()));
        }
        if (fileState.getLastTnxId() > txId.getId())
            sendBackLogMessage(message, fileState, rState, txId.getId());
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


    @Override
    public void close() throws IOException {

    }
}
