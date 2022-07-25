package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.InvalidTransactionError;
import ai.sapper.hcdc.agents.common.TransactionProcessor;
import ai.sapper.hcdc.agents.namenode.HDFSSnapshotProcessor;
import ai.sapper.hcdc.agents.namenode.model.DFSBlockReplicaState;
import ai.sapper.hcdc.agents.namenode.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.namenode.model.NameNodeTxState;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import ai.sapper.hcdc.core.io.FSBlock;
import ai.sapper.hcdc.core.io.FSFile;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.hcdc.core.messaging.InvalidMessageError;
import ai.sapper.hcdc.core.messaging.MessageObject;
import ai.sapper.hcdc.core.messaging.MessageSender;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileState;
import ai.sapper.hcdc.core.model.HDFSBlockData;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.hadoop.hdfs.HDFSBlockReader;

import java.util.List;

public class FileTransactionProcessor extends TransactionProcessor {
    private MessageSender<String, DFSChangeDelta> sender;
    private FileSystem fs;
    private HdfsConnection connection;

    public FileTransactionProcessor withHdfsConnection(@NonNull HdfsConnection connection) {
        this.connection = connection;
        return this;
    }

    public FileTransactionProcessor withSenderQueue(@NonNull MessageSender<String, DFSChangeDelta> sender) {
        this.sender = sender;
        return this;
    }

    public FileTransactionProcessor withFileSystem(@NonNull FileSystem fs) {
        this.fs = fs;
        return this;
    }

    /**
     * @param message
     * @return
     * @throws Exception
     */
    @Override
    public long checkMessageSequence(MessageObject<String, DFSChangeDelta> message) throws Exception {
        long txId = Long.parseLong(message.value().getTxId());
        if (message.mode() == MessageObject.MessageMode.New) {
            NameNodeTxState txState = stateManager().agentTxState();
            if (txId <= txState.getProcessedTxId()) {
                throw new InvalidMessageError(message.id(), String.format("Duplicate message: Transaction already processed. [TXID=%d][CURRENT=%d]", txId, txState.getProcessedTxId()));
            }
        }
        return txId;
    }

    private void sendIgnoreTx(MessageObject<String, DFSChangeDelta> message, Object data) throws Exception {
        // Do nothing...
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAddFileTxMessage(DFSAddFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());
        registerFile(data.getFile().getPath(), schemaEntity, message.mode());
    }

    private DFSFileReplicaState registerFile(String hdfsPath, SchemaEntity schemaEntity, MessageObject.MessageMode mode) throws Exception {
        DFSFileState fileState = stateManager().get(hdfsPath);
        if (fileState == null) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    hdfsPath,
                    String.format("File not found. [path=%s]", hdfsPath));
        }

        FSFile file = FileSystemHelper.createFile(fileState, fs, schemaEntity);
        stateManager().replicationLock().lock();
        try {
            DFSFileReplicaState rState = stateManager().create(fileState.getId(), fileState.getHdfsFilePath(), schemaEntity, true);
            rState.setSnapshotTxId(fileState.getLastTnxId());
            rState.setSnapshotTime(System.currentTimeMillis());
            rState.setSnapshotReady(mode != MessageObject.MessageMode.Snapshot);
            rState.setState(fileState.getState());
            for (DFSBlockState bs : fileState.getBlocks()) {
                DFSBlockReplicaState b = new DFSBlockReplicaState();
                b.setState(EFileState.New);
                b.setBlockId(bs.getBlockId());
                b.setPrevBlockId(bs.getPrevBlockId());
                b.setStartOffset(0);
                b.setDataSize(bs.getDataSize());
                b.setUpdateTime(System.currentTimeMillis());
                rState.add(b);
            }
            stateManager().update(rState);

            return rState;
        } finally {
            stateManager().replicationLock().unlock();
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processAppendFileTxMessage(DFSAppendFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
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
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            stateManager().replicationLock().lock();
            try {
                rState = stateManager().get(fileState.getId());
                rState.setState(EFileState.Updating);

                rState = stateManager().update(rState);
            } finally {
                stateManager().replicationLock().unlock();
            }
            LOG.debug(String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processDeleteFileTxMessage(DFSDeleteFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
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
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());


        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.isEnabled()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            file.delete();
            stateManager().replicationLock().lock();
            try {
                rState = stateManager().get(fileState.getId());
                stateManager().delete(rState.getInode());
            } finally {
                stateManager().replicationLock().unlock();
            }
            LOG.debug(String.format("Deleted file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
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
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
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
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.canUpdate()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            for (DFSBlockState block : fileState.getBlocks()) {
                FSBlock b = file.get(block.getBlockId());
                if (b == null) {
                    file.add(block);
                }
            }
            LOG.debug(String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null || !rState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processUpdateBlocksTxMessage(DFSUpdateBlocks data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
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
        List<DFSBlock> blocks = data.getBlocksList();
        if (blocks.isEmpty()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("File State out of sync, no block to update. [path=%s]",
                            fileState.getHdfsFilePath()));
        }
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.canUpdate()) {
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
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
                FSBlock b = file.get(bs.getBlockId());
                if (b == null) {
                    throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                            fileState.getHdfsFilePath(),
                            String.format("File State out of sync, block not found. [path=%s][blockID=%d]",
                                    fileState.getHdfsFilePath(), block.getBlockId()));
                }
            }
            LOG.debug(String.format("Updating file. [path=%s]", fileState.getHdfsFilePath()));
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null || !rState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processTruncateBlockTxMessage(DFSTruncateBlock data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
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
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processCloseFileTxMessage(DFSCloseFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        SchemaEntity schemaEntity = new SchemaEntity();
        schemaEntity.setDomain(message.value().getNamespace());
        schemaEntity.setEntity(message.value().getEntityName());
        if (message.mode() == MessageObject.MessageMode.Snapshot) {
            registerFile(data.getFile().getPath(), schemaEntity, message.mode());
        }
        DFSFileState fileState = stateManager().get(data.getFile().getPath());
        if (fileState == null || !fileState.canProcess()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("NameNode Replica out of sync, missing file state. [path=%s]",
                            data.getFile().getPath()));
        }
        if (fileState.getLastTnxId() >= txId && message.mode() != MessageObject.MessageMode.Snapshot) {
            LOG.warn(String.format("Duplicate transaction message: [message ID=%s][mode=%s]",
                    message.id(), message.mode().name()));
            return;
        }

        DFSFileReplicaState rState = stateManager().get(fileState.getId());
        if (!fileState.hasError() && rState != null && rState.canUpdate()) {
            HDFSBlockReader reader = new HDFSBlockReader(connection.dfsClient(), rState.getHdfsPath());
            FSFile file = fs.get(fileState, fs, schemaEntity);
            if (file == null) {
                throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                        data.getFile().getPath(),
                        String.format("FileSystem file not found. [path=%s]",
                                data.getFile().getPath()));
            }
            List<DFSBlock> blocks = data.getBlocksList();
            if (!blocks.isEmpty()) {
                for (DFSBlock block : blocks) {
                    DFSBlockReplicaState bs = rState.get(block.getBlockId());
                    if (bs == null) {
                        throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                                data.getFile().getPath(),
                                String.format("Block not registered for update. [path=%s][block ID=%d]",
                                        data.getFile().getPath(), block.getBlockId()));
                    }
                    FSBlock fsb = file.get(block.getBlockId());
                    if (fsb == null) {
                        throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                                data.getFile().getPath(),
                                String.format("Block not found in FileSystem. [path=%s][block ID=%d]",
                                        data.getFile().getPath(), block.getBlockId()));
                    }
                    long size = copyBlock(block, rState, bs, fsb, reader);
                }
            }
            stateManager().replicationLock().lock();
            try {
                rState = stateManager().get(fileState.getId());
                rState.clear();
                rState.setState(EFileState.Finalized);

                rState = stateManager().update(rState);
            } finally {
                stateManager().replicationLock().unlock();
            }
        } else if (fileState.hasError()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsFilePath(),
                    String.format("FileSystem sync error. [path=%s]", fileState.getHdfsFilePath()));
        } else if (rState == null || !rState.canUpdate()) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    data.getFile().getPath(),
                    String.format("File not setup for replication. [path=%s]",
                            data.getFile().getPath()));
        }
    }

    private long copyBlock(DFSBlock source,
                           DFSFileReplicaState fileState,
                           DFSBlockReplicaState blockState,
                           FSBlock fsBlock,
                           HDFSBlockReader reader) throws Exception {
        HDFSBlockData data = reader.read(source.getBlockId(),
                source.getGenerationStamp(),
                0L,
                (int) source.getEndOffset());
        if (data == null) {
            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                    fileState.getHdfsPath(),
                    String.format("Error reading block from HDFS. [path=%s][block ID=%d]",
                            fileState.getHdfsPath(), source.getBlockId()));
        }
        fsBlock.write(data.data().array());

        return data.dataSize();
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processRenameFileTxMessage(DFSRenameFile data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        throw new InvalidMessageError(message.id(), "Rename transaction should not come...");
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processIgnoreTxMessage(DFSIgnoreTx data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        LOG.debug(String.format("Received Ignore Transaction: [ID=%d]", txId));
    }

    /**
     * @param data
     * @param message
     * @param txId
     * @throws Exception
     */
    @Override
    public void processErrorTxMessage(DFSError data, MessageObject<String, DFSChangeDelta> message, long txId) throws Exception {
        LOG.error(String.format("Received Error Message: %s. [TX=%d][ERROR CODE=%s]", data.getMessage(), txId, data.getCode().name()));
    }

    /**
     * @param message
     * @param data
     * @param te
     * @throws Exception
     */
    @Override
    public void processErrorMessage(MessageObject<String, DFSChangeDelta> message, Object data, InvalidTransactionError te) throws Exception {
        if (!Strings.isNullOrEmpty(te.getHdfsPath())) {
            DFSFileState fileState = stateManager().get(te.getHdfsPath());
            if (fileState != null) {
                stateManager().updateState(fileState.getHdfsFilePath(), EFileState.Error);
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
}
