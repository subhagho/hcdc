package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.connections.HdfsConnection;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.messaging.InvalidMessageError;
import ai.sapper.hcdc.core.messaging.MessageObject;
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

import java.util.List;

@Getter
@Accessors(fluent = true)
public class FileDeltaProcessor extends ChangeDeltaProcessor {
    private static Logger LOG = LoggerFactory.getLogger(FileDeltaProcessor.class);
    private FileTransactionProcessor processor = new FileTransactionProcessor();
    private long receiveBatchTimeout = 1000;
    private FileSystem fs;
    private FileDeltaProcessorConfig config;
    private HdfsConnection connection;
    private FileSystem.FileSystemMocker fileSystemMocker;


    public FileDeltaProcessor(@NonNull ZkStateManager stateManager) {
        super(stateManager);
    }

    public FileDeltaProcessor withMockFileSystem(@NonNull FileSystem.FileSystemMocker fileSystemMocker) {
        this.fileSystemMocker = fileSystemMocker;
        return this;
    }

    /**
     * @param xmlConfig
     * @param manger
     * @return
     * @throws ConfigurationException
     */
    @Override
    public ChangeDeltaProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                     @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            config = new FileDeltaProcessorConfig(xmlConfig);
            config.read();

            super.init(config, manger);

            connection = manger.getConnection(config.hdfsConnection, HdfsConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("HDFS Connection not found. [name=%s]", config.hdfsConnection));
            }
            if (!connection.isConnected()) connection.connect();

            if (fileSystemMocker == null) {
                Class<? extends FileSystem> fsc = (Class<? extends FileSystem>) Class.forName(config.fsType);
                fs = fsc.newInstance();
                fs.init(config.config(), FileDeltaProcessorConfig.Constants.CONFIG_PATH_FS);
            } else {
                fs = fileSystemMocker.create(config.config());
            }

            processor.withFileSystem(fs)
                    .withHdfsConnection(connection)
                    .withSenderQueue(sender())
                    .withStateManager(stateManager())
                    .withErrorQueue(errorSender());

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
        try {
            run(false);
        } catch (Exception ex) {
            LOG.error(ex.getLocalizedMessage(), ex);
        }
    }

    public void run(boolean once) throws Exception {
        Preconditions.checkState(sender() != null);
        Preconditions.checkState(receiver() != null);
        Preconditions.checkState(errorSender() != null);
        Preconditions.checkArgument(fs != null);
        try {
            while (NameNodeEnv.get().state().isAvailable()) {
                List<MessageObject<String, DFSChangeDelta>> batch = receiver().nextBatch(receiveBatchTimeout);
                if (batch == null || batch.isEmpty()) {
                    if (once) break;
                    Thread.sleep(receiveBatchTimeout);
                    continue;
                }
                LOG.debug(String.format("Received messages. [count=%d]", batch.size()));
                for (MessageObject<String, DFSChangeDelta> message : batch) {
                    try {
                        long txId = process(message);
                        if (txId > 0) {
                            if (message.mode() == MessageObject.MessageMode.New) {
                                stateManager().update(txId);
                                LOG.debug(String.format("Processed transaction delta. [TXID=%d]", txId));
                            } else if (message.mode() == MessageObject.MessageMode.Snapshot) {
                                if (stateManager().agentTxState().getProcessedTxId() < txId) {
                                    stateManager().update(txId);
                                    LOG.debug(String.format("Processed transaction delta. [TXID=%d]", txId));
                                }
                            }
                        }
                    } catch (InvalidMessageError ie) {
                        LOG.error("Error processing message.", ie);
                        DefaultLogger.stacktrace(LOG, ie);
                        errorSender().send(message);
                    }
                    receiver().ack(message.id());
                }
            }
            LOG.warn(String.format("Delta Change Processor thread stopped. [env state=%s]", NameNodeEnv.get().state().state().name()));
        } catch (Throwable t) {
            LOG.error("Delta Change Processor terminated with error", t);
            DefaultLogger.stacktrace(LOG, t);
        }
    }

    private long process(MessageObject<String, DFSChangeDelta> message) throws Exception {
        long txId = -1;
        if (!isValidMessage(message)) {
            throw new InvalidMessageError(message.id(),
                    String.format("Invalid Message mode. [id=%s][mode=%s]", message.id(), message.mode().name()));
        }
        txId = processor.checkMessageSequence(message, true);

        processor.processTxMessage(message, txId);

        return txId;
    }

    private boolean isValidMessage(MessageObject<String, DFSChangeDelta> message) {
        boolean ret = false;
        if (message.mode() != null) {
            ret = (message.mode() == MessageObject.MessageMode.New
                    || message.mode() == MessageObject.MessageMode.Backlog
                    || message.mode() == MessageObject.MessageMode.Snapshot);
        }
        if (ret) {
            ret = message.value().hasTxId();
        }
        return ret;
    }

    @Getter
    @Accessors(fluent = true)
    public static class FileDeltaProcessorConfig extends ChangeDeltaProcessorConfig {
        public static final String __CONFIG_PATH = "processor.files";

        public static class Constants {
            public static final String CONFIG_PATH_FS = "filesystem";
            public static final String CONFIG_FS_TYPE = String.format("%s.type", CONFIG_PATH_FS);
            public static final String CONFIG_HDFS_CONN = "hdfs";
        }

        private String fsType;
        private String hdfsConnection;
        private HierarchicalConfiguration<ImmutableNode> fsConfig;

        public FileDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        /**
         * @throws ConfigurationException
         */
        @Override
        public void read() throws ConfigurationException {
            super.read();

            fsConfig = get().configurationAt(Constants.CONFIG_PATH_FS);
            if (fsConfig == null) {
                throw new ConfigurationException(
                        String.format("File Processor Error: missing configuration. [path=%s]",
                                Constants.CONFIG_PATH_FS));
            }
            fsType = get().getString(Constants.CONFIG_FS_TYPE);
            if (Strings.isNullOrEmpty(fsType)) {
                throw new ConfigurationException(
                        String.format("File Processor Error: missing configuration. [name=%s]",
                                Constants.CONFIG_FS_TYPE));
            }
            hdfsConnection = get().getString(Constants.CONFIG_HDFS_CONN);
            if (Strings.isNullOrEmpty(hdfsConnection)) {
                throw new ConfigurationException(
                        String.format("File Processor Error: missing configuration. [name=%s]",
                                Constants.CONFIG_HDFS_CONN));
            }
        }
    }
}
