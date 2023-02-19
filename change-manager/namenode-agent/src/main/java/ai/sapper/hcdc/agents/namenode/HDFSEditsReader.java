package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.messaging.HCDCMessagingBuilders;
import ai.sapper.cdc.core.messaging.MessageSender;
import ai.sapper.cdc.core.messaging.MessagingConfig;
import ai.sapper.cdc.core.model.CDCAgentState;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.parquet.Strings;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public abstract class HDFSEditsReader implements Runnable {
    protected final String name;
    protected final ZkStateManager stateManager;
    protected MessageSender<String, DFSChangeDelta> sender;
    protected HDFSEditsReaderConfig processorConfig;

    public HDFSEditsReader(@NonNull String name, @NonNull ZkStateManager stateManager) {
        this.name = name;
        this.stateManager = stateManager;
    }

    public void setup(@NonNull ConnectionManager manger) throws Exception {
        Preconditions.checkNotNull(processorConfig);
        sender = new HCDCMessagingBuilders.SenderBuilder()
                .config(processorConfig.senderConfig.config())
                .manager(manger)
                .connection(processorConfig.senderConfig.connection())
                .type(processorConfig.senderConfig.type())
                .partitioner(processorConfig.senderConfig.partitionerClass())
                .auditLogger(NameNodeEnv.get(name).auditLogger())
                .build();
        if (NameNodeEnv.get(name).hadoopConfig() == null) {
            throw new ConfigurationException("Hadoop Configuration not initialized...");
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
        try {
            NameNodeEnv env = NameNodeEnv.get(name);
            Preconditions.checkNotNull(env);
            Preconditions.checkState(sender != null);
            try {
                NameNodeEnv.get(name).agentState().state(CDCAgentState.EAgentState.Active);
                while (NameNodeEnv.get(name).state().isAvailable()) {
                    long txid = doRun();
                    LOGGER.info(getClass(), txid, String.format("Last Processed TXID = %d", txid));
                    Thread.sleep(processorConfig.pollingInterval());
                }
                NameNodeEnv.get(name).agentState().state(CDCAgentState.EAgentState.Stopped);
            } catch (Throwable t) {
                try {
                    DefaultLogger.error(env.LOG, "Edits Log Processor terminated with error", t);
                    DefaultLogger.stacktrace(env.LOG, t);
                    NameNodeEnv.get(name).agentState().error(t);
                } catch (Exception ex) {
                    DefaultLogger.error(env.LOG, ex.getLocalizedMessage());
                }
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.LOGGER.error(t.getLocalizedMessage());
        }
    }


    public abstract HDFSEditsReader init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         @NonNull ConnectionManager manger) throws ConfigurationException;

    public abstract long doRun() throws Exception;

    @Getter
    @Accessors(fluent = true)
    public static class HDFSEditsReaderConfig extends ConfigReader {
        public static class Constants {
            public static final String __CONFIG_PATH = "processor.edits";
            public static final String CONFIG_Q_CONNECTION = "sender";
            public static final String CONFIG_POLL_INTERVAL = "pollingInterval";
            public static final String CONFIG_LOCK_TIMEOUT = "lockTimeout";
        }

        private MessagingConfig senderConfig;
        private long pollingInterval = 60000; // By default, run every minute
        private long defaultLockTimeout = 5 * 60 * 1000; // 5 mins.

        public HDFSEditsReaderConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
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
                if (get().containsKey(Constants.CONFIG_POLL_INTERVAL)) {
                    String s = get().getString(Constants.CONFIG_POLL_INTERVAL);
                    if (!Strings.isNullOrEmpty(s)) {
                        pollingInterval = Long.parseLong(s);
                    }
                }
                if (get().containsKey(Constants.CONFIG_LOCK_TIMEOUT)) {
                    String s = get().getString(Constants.CONFIG_LOCK_TIMEOUT);
                    if (!Strings.isNullOrEmpty(s)) {
                        defaultLockTimeout = Long.parseLong(s);
                    }
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
