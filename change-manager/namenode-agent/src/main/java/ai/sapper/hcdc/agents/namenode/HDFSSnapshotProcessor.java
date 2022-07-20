package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.namenode.model.DFSReplicationState;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.messaging.*;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public class HDFSSnapshotProcessor {
    private final ZkStateManager stateManager;
    private MessageSender<String, DFSChangeDelta> sender;
    private MessageSender<String, DFSChangeDelta> tnxSender;
    private HDFSSnapshotProcessorConfig processorConfig;

    public HDFSSnapshotProcessor(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
    }

    public HDFSSnapshotProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                      @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            processorConfig = new HDFSSnapshotProcessorConfig(xmlConfig);
            processorConfig.read();

            sender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.senderConfig.config())
                    .manager(manger)
                    .connection(processorConfig().senderConfig.connection())
                    .type(processorConfig().senderConfig.type())
                    .partitioner(processorConfig().senderConfig.partitionerClass())
                    .build();

            tnxSender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.tnxSenderConfig.config())
                    .manager(manger)
                    .connection(processorConfig().tnxSenderConfig.connection())
                    .type(processorConfig().tnxSenderConfig.type())
                    .partitioner(processorConfig().tnxSenderConfig.partitionerClass())
                    .build();
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void snapshot(@NonNull String hdfsPath) throws SnapshotError {
        Preconditions.checkState(sender != null);
        try {
            DefaultLogger.LOG.info(String.format("Generating snapshot for file. [path=%s]", hdfsPath));
            DFSFileState fileState = stateManager.get(hdfsPath);
            if (fileState == null) {
                throw new SnapshotError(String.format("HDFS File State not found. [path=%s]", hdfsPath));
            }
            DFSReplicationState rState = stateManager.get(fileState.getId());
            if (rState == null) {
                rState = stateManager.create(fileState.getId(), fileState.getHdfsFilePath(), true);
            }

            DFSAddFile addFile = generateSnapshot(fileState, true);
            MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(NameNodeEnv.get().namespace(),
                    addFile, DFSAddFile.class, MessageObject.MessageMode.Snapshot);
            sender.send(message);

            rState.setSnapshotTxId(fileState.getLastTnxId());
            rState.setSnapshotTime(System.currentTimeMillis());
            stateManager.update(rState);

            DefaultLogger.LOG.info(String.format("Snapshot generated for path. [path=%s][inode=%d]", fileState.getHdfsFilePath(), fileState.getId()));
        } catch (SnapshotError se) {
            throw se;
        } catch (Exception ex) {
            throw new SnapshotError(ex);
        }
    }

    public void snapshotReady(@NonNull String hdfsPath, long tnxId) throws SnapshotError {
        Preconditions.checkState(tnxSender != null);
        try {
            DFSFileState fileState = stateManager.get(hdfsPath);
            if (fileState == null) {
                throw new SnapshotError(String.format("HDFS File State not found. [path=%s]", hdfsPath));
            }
            DFSReplicationState rState = stateManager.get(fileState.getId());
            if (rState == null) {
                throw new SnapshotError(String.format("HDFS File replication record not found. [path=%s]", hdfsPath));
            }
            if (tnxId != rState.getSnapshotTxId()) {
                throw new SnapshotError(String.format("Snapshot transaction mismatch. [expected=%d][actual=%d]", rState.getSnapshotTxId(), tnxId));
            }
            DFSAddFile addFile = generateSnapshot(fileState, true);
            MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(NameNodeEnv.get().namespace(),
                    addFile, DFSAddFile.class, MessageObject.MessageMode.Backlog);
            tnxSender.send(message);
        } catch (SnapshotError se) {
            throw se;
        } catch (Exception ex) {
            throw new SnapshotError(ex);
        }
    }

    public static DFSAddFile generateSnapshot(DFSFileState state, boolean addBlocks) throws Exception {
        DFSTransaction tx = DFSTransaction.newBuilder()
                .setOp(DFSTransaction.Operation.ADD_FILE)
                .setTransactionId(state.getLastTnxId())
                .setTimestamp(state.getUpdatedTime())
                .build();
        DFSFile file = DFSFile.newBuilder()
                .setInodeId(state.getId())
                .setPath(state.getHdfsFilePath())
                .build();
        DFSAddFile.Builder builder = DFSAddFile.newBuilder();
        builder.setOverwrite(false)
                .setModifiedTime(state.getUpdatedTime())
                .setBlockSize(state.getBlockSize())
                .setFile(file)
                .setTransaction(tx)
                .setLength(state.getDataSize())
                .setAccessedTime(state.getUpdatedTime());
        if (addBlocks) {
            for (DFSBlockState block : state.sortedBlocks()) {
                DFSBlock b = generateBlockSnapshot(block);
                builder.addBlocks(b);
            }
        }
        return builder.build();
    }

    public static DFSBlock generateBlockSnapshot(DFSBlockState block) throws Exception {
        DFSBlock.Builder builder = DFSBlock.newBuilder();
        builder.setBlockId(block.getBlockId())
                .setGenerationStamp(block.getGenerationStamp())
                .setSize(block.getDataSize())
                .setBlockSize(block.getBlockSize());
        return builder.build();
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class HDFSSnapshotProcessorConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "snapshot.manager";
        private static final String __CONFIG_PATH_SENDER = "sender";
        private static final String __CONFIG_PATH_TNX_SENDER = "tnxSender";

        private MessagingConfig senderConfig;
        private MessagingConfig tnxSenderConfig;

        public HDFSSnapshotProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public HDFSSnapshotProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, path);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                HierarchicalConfiguration<ImmutableNode> config = get().configurationAt(__CONFIG_PATH_SENDER);
                if (config == null) {
                    throw new ConfigurationException(String.format("Sender configuration node not found. [path=%s]", __CONFIG_PATH_SENDER));
                }
                senderConfig = new MessagingConfig();
                senderConfig.read(config);

                config = get().configurationAt(__CONFIG_PATH_TNX_SENDER);
                if (config == null) {
                    throw new ConfigurationException(String.format("Sender configuration node not found. [path=%s]", __CONFIG_PATH_TNX_SENDER));
                }
                tnxSenderConfig = new MessagingConfig();
                tnxSenderConfig.read(config);
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}