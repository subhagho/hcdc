package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.common.DFSEditsFileFinder;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.model.DFSEditLogBatch;
import ai.sapper.hcdc.agents.model.DFSTransactionType;
import ai.sapper.hcdc.agents.model.NameNodeAgentState;
import ai.sapper.hcdc.agents.model.NameNodeTxState;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.messaging.*;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.EditsLogReader;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class EditLogProcessor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(EditLogProcessor.class.getCanonicalName());
    private static final String PATH_NN_CURRENT_DIR = "current";

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
                    .build();
            if (NameNodeEnv.get().hadoopConfig() == null) {
                throw new ConfigurationException("Hadoop Configuration not initialized...");
            }
            String edir = NameNodeEnv.get().hadoopConfig().nameNodeEditsDir();
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
            NameNodeEnv.get().agentState().state(NameNodeAgentState.EAgentState.Active);
            while (NameNodeEnv.get().state().isAvailable()) {
                long txid = doRun();
                LOG.info(String.format("Last Processed TXID = %d", txid));
                Thread.sleep(processorConfig.pollingInterval);
            }
            NameNodeEnv.get().agentState().state(NameNodeAgentState.EAgentState.Stopped);
        } catch (Throwable t) {
            LOG.error("Edits Log Processor terminated with error", t);
            DefaultLogger.stacktrace(LOG, t);
            NameNodeEnv.get().agentState().error(t);
        }
    }


    public long doRun() throws Exception {
        NameNodeTxState state = stateManager.agentTxState();
        EditsLogReader reader = new EditsLogReader();
        long txId = state.getProcessedTxId();
        List<DFSEditsFileFinder.EditsLogFile> files = DFSEditsFileFinder
                .findEditsFiles(getPathNnCurrentDir(editsDir.getAbsolutePath()),
                        state.getProcessedTxId() + 1, -1);
        if (files != null && !files.isEmpty()) {
            for (DFSEditsFileFinder.EditsLogFile file : files) {
                LOG.debug(String.format("Reading edits file [path=%s][startTx=%d]", file, state.getProcessedTxId()));
                reader.run(file, state.getProcessedTxId(), file.endTxId());
                DFSEditLogBatch batch = reader.batch();
                if (batch.transactions() != null && !batch.transactions().isEmpty()) {
                    long tid = processBatch(batch, txId);
                    if (tid > 0) {
                        txId = tid;
                        stateManager.update(txId);
                    }
                }
            }
        }
        String cf = DFSEditsFileFinder.getCurrentEditsFile(getPathNnCurrentDir(editsDir.getAbsolutePath()));
        if (cf == null) {
            throw new Exception(String.format("Current Edits file not found. [dir=%s]",
                    editsDir.getAbsolutePath()));
        }
        long ltx = DFSEditsFileFinder.findSeenTxID(getPathNnCurrentDir(editsDir.getAbsolutePath()));
        LOG.info(String.format("Current Edits File: %s, Last Seen TXID=%d", cf, ltx));

        return txId;
    }

    private long processBatch(DFSEditLogBatch batch, long lastTxId) throws Exception {
        if (batch != null && batch.transactions() != null && !batch.transactions().isEmpty()) {
            long txid = -1;
            for (DFSTransactionType<?> tnx : batch.transactions()) {
                if (tnx.id() <= lastTxId) continue;
                Object proto = tnx.convertToProto();
                MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(NameNodeEnv.get().source(),
                        proto,
                        proto.getClass(),
                        null,
                        null,
                        MessageObject.MessageMode.New);
                sender.send(message);
                txid = tnx.id();
            }
            return txid;
        }
        return -1;
    }

    private String getPathNnCurrentDir(String path) {
        return String.format("%s/%s", path, PATH_NN_CURRENT_DIR);
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
        private long pollingInterval = 60000; // By default, run every minute

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
