package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.agents.namenode.NameNodeEnv;
import ai.sapper.hcdc.agents.namenode.model.DFSReplicationState;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.state.DFSBlockState;
import ai.sapper.hcdc.core.connections.state.DFSFileState;
import ai.sapper.hcdc.core.messaging.*;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

@Getter
@Accessors(fluent = true)
public class HDFSSnapshotManager {
    private final ZkStateManager stateManager;
    private MessageSender<String, DFSChangeDelta> sender;
    private HDFSSnapshotManagerConfig managerConfig;

    public HDFSSnapshotManager(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
    }

    public HDFSSnapshotManager init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                    @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            managerConfig = new HDFSSnapshotManagerConfig(xmlConfig);
            managerConfig.read();

            sender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(managerConfig.config())
                    .manager(manger)
                    .connection(managerConfig().mConfig.connection())
                    .type(managerConfig().mConfig.type())
                    .partitioner(managerConfig().mConfig.partitionerClass())
                    .topic(managerConfig().mConfig.topic())
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

            DFSAddFile addFile = generateSnapshot(fileState);
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

    private DFSAddFile generateSnapshot(DFSFileState state) throws Exception {
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
        for (DFSBlockState block : state.sortedBlocks()) {
            DFSBlock b = generateBlockSnapshot(block);
            builder.addBlocks(b);
        }
        return builder.build();
    }

    private DFSBlock generateBlockSnapshot(DFSBlockState block) throws Exception {
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
    public static class HDFSSnapshotManagerConfig extends ConfigReader {
        private static class Constants {
            private static final String CONFIG_CONNECTION_TYPE = "connectionType";
            private static final String CONFIG_CONNECTION = "connection";
            private static final String CONFIG_TOPIC = "topic";
            private static final String CONFIG_PARTITIONER_CLASS = "partitioner";
        }

        private static final String __CONFIG_PATH = "snapshot.manager";

        private MessagingConfig mConfig;

        public HDFSSnapshotManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public HDFSSnapshotManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, path);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                mConfig = new MessagingConfig();
                mConfig.read(get());
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
