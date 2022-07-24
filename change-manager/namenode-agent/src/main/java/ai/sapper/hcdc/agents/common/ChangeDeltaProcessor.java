package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.agents.namenode.model.NameNodeTxState;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.messaging.HCDCMessagingBuilders;
import ai.sapper.hcdc.core.messaging.MessageReceiver;
import ai.sapper.hcdc.core.messaging.MessageSender;
import ai.sapper.hcdc.core.messaging.MessagingConfig;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Accessors(fluent = true)
public abstract class ChangeDeltaProcessor implements Runnable {
    private final ZkStateManager stateManager;
    private ChangeDeltaProcessorConfig processorConfig;
    private MessageSender<String, DFSChangeDelta> sender;
    private MessageSender<String, DFSChangeDelta> errorSender;
    private MessageReceiver<String, DFSChangeDelta> receiver;
    private long receiveBatchTimeout = 1000;

    public ChangeDeltaProcessor(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
    }

    public ChangeDeltaProcessor init(@NonNull ChangeDeltaProcessorConfig config,
                                     @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            this.processorConfig = config;
            processorConfig.read();

            sender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.senderConfig.config())
                    .manager(manger)
                    .connection(processorConfig().senderConfig.connection())
                    .type(processorConfig().senderConfig.type())
                    .partitioner(processorConfig().senderConfig.partitionerClass())
                    .build();

            receiver = new HCDCMessagingBuilders.ReceiverBuilder()
                    .config(processorConfig().receiverConfig.config())
                    .manager(manger)
                    .connection(processorConfig.receiverConfig.connection())
                    .type(processorConfig.receiverConfig.type())
                    .saveState(true)
                    .zkConnection(stateManager().connection())
                    .zkStatePath(stateManager.zkPath())
                    .batchSize(processorConfig.receiverConfig.batchSize())
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
                    .build();

            long txId = stateManager().getSnapshotTxId();
            NameNodeTxState state = stateManager.agentTxState();
            if (txId > state.getProcessedTxId()) {
                state = stateManager.update(txId);
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ChangeDeltaProcessorConfig extends ConfigReader {
        public static class Constants {
            public static final String __CONFIG_PATH = "processor.cdc";
            public static final String __CONFIG_PATH_SENDER = "sender";
            public static final String __CONFIG_PATH_RECEIVER = "receiver";
            public static final String __CONFIG_PATH_ERROR = "errorQueue";
            public static final String CONFIG_RECEIVE_TIMEOUT = "readBatchTimeout";
        }

        private MessagingConfig senderConfig;
        private MessagingConfig receiverConfig;
        private MessagingConfig errorConfig;
        private String batchTimeout;

        public ChangeDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Constants.__CONFIG_PATH);
        }

        public ChangeDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, path);
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
