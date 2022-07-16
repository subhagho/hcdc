package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.state.DFSFileState;
import ai.sapper.hcdc.core.messaging.HCDCMessagingBuilders;
import ai.sapper.hcdc.core.messaging.MessageSender;
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
                    .connection(managerConfig().connection)
                    .type(managerConfig().type)
                    .partitioner(managerConfig().partitioner)
                    .topic(managerConfig().topic)
                    .build();
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void snapshot(@NonNull String hdfsPath) throws SnapshotError {
        Preconditions.checkState(sender != null);
        try {
            DFSFileState fileState = stateManager.get(hdfsPath);
            if (fileState == null) {
                throw new SnapshotError(String.format("HDFS File State not found. [path=%s]", hdfsPath));
            }
            if (fileState.getSnapshotTxId() > 0) {
                DefaultLogger.LOG.warn(String.format("Snapshot already scheduled or processed. [path=%s]", hdfsPath));
            }
        } catch (SnapshotError se) {
            throw se;
        } catch (Exception ex) {
            throw new SnapshotError(ex);
        }
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

        private String type;
        private String connection;
        private String topic;
        private String partitioner;

        public HDFSSnapshotManagerConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                type = get().getString(Constants.CONFIG_CONNECTION_TYPE);
                if (Strings.isNullOrEmpty(type)) {
                    throw new ConfigurationException(String.format("Snapshot Manager Manager Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION_TYPE));
                }
                connection = get().getString(Constants.CONFIG_CONNECTION);
                if (Strings.isNullOrEmpty(connection)) {
                    throw new ConfigurationException(String.format("Snapshot Manager Manager Configuration Error: missing [%s]", Constants.CONFIG_CONNECTION));
                }
                if (get().containsKey(Constants.CONFIG_TOPIC))
                    topic = get().getString(Constants.CONFIG_TOPIC);
                if (get().containsKey(Constants.CONFIG_PARTITIONER_CLASS))
                    partitioner = get().getString(Constants.CONFIG_PARTITIONER_CLASS);
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
