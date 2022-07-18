package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.messaging.HCDCMessagingBuilders;
import ai.sapper.hcdc.core.messaging.MessageSender;
import ai.sapper.hcdc.core.messaging.MessagingConfig;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

@Getter
@Accessors(fluent = true)
public class EditLogProcessor implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(EditLogProcessor.class.getCanonicalName());

    private final ZkStateManager stateManager;
    private MessageSender<String, DFSChangeDelta> sender;
    private EditLogProcessorConfig processorConfig;
    private File editsDir;

    public EditLogProcessor(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
    }

    public EditLogProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                 @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            processorConfig = new EditLogProcessorConfig(xmlConfig);
            processorConfig.read();

            sender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.senderConfig.config())
                    .manager(manger)
                    .connection(processorConfig().senderConfig.connection())
                    .type(processorConfig().senderConfig.type())
                    .partitioner(processorConfig().senderConfig.partitionerClass())
                    .topic(processorConfig().senderConfig.topic())
                    .build();
            String edir = NameNodeEnv.get().config().nameNodeEditsDir();
            editsDir = new File(edir);
            if (!editsDir.exists()) {
                throw new ConfigurationException(
                        String.format("Invalid Hadoop Configuration: Edits directory not found. [path=%s]",
                                editsDir.getAbsolutePath()));
            }

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
        try {
            while (NameNodeEnv.get().state().isAvailable()) {
                stateManager.agentTxState();
            }
        } catch (Throwable t) {
            LOG.error("Delta Change Processor terminated with error", t);
            DefaultLogger.stacktrace(LOG, t);
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class EditLogProcessorConfig extends ConfigReader {
        public static class Constants {
            public static final String __CONFIG_PATH = "processor.edits";
            public static final String CONFIG_Q_CONNECTION = "sender";
            public static final String CONFIG_POLL_INTERVAL = "pollingInterval";
        }

        private MessagingConfig senderConfig;
        private long pollingInterval = -1;

        public EditLogProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, Constants.__CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                HierarchicalConfiguration<ImmutableNode> config = get().configurationAt(Constants.CONFIG_Q_CONNECTION);
                if (config == null) {
                    throw new ConfigurationException(String.format("Sender configuration node not found. [path=%s]", Constants.CONFIG_Q_CONNECTION));
                }
                senderConfig = new MessagingConfig();
                senderConfig.read(config);
                String s = get().getString(Constants.CONFIG_POLL_INTERVAL);
                if (!Strings.isNullOrEmpty(s)) {
                    pollingInterval = Long.parseLong(s);
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
