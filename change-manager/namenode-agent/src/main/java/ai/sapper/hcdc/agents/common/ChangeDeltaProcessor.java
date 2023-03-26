package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.ManagerStateError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.messaging.*;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.cdc.core.model.CDCAgentState;
import ai.sapper.cdc.core.model.LongTxState;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSTransaction;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.MessageOrBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public abstract class ChangeDeltaProcessor implements Runnable, Closeable {
    public enum EProcessorMode {
        Reader, Committer
    }

    public static final int LOCK_RETRY_COUNT = 16;

    private static Logger LOG;

    private final String name;
    private final ZkStateManager stateManager;
    private ChangeDeltaProcessorConfig processorConfig;
    private MessageSender<String, DFSChangeDelta> sender;
    private MessageSender<String, DFSChangeDelta> errorSender;
    private MessageReceiver<String, DFSChangeDelta> receiver;
    private long receiveBatchTimeout = 1000;
    private NameNodeEnv env;
    private String processedMessageId = null;
    private TransactionProcessor processor;
    private final EProcessorMode mode;
    private final boolean ignoreMissing;
    private DistributedLock __lock;

    public ChangeDeltaProcessor(@NonNull ZkStateManager stateManager,
                                @NonNull String name,
                                @NonNull EProcessorMode mode,
                                boolean ignoreMissing) {
        this.stateManager = stateManager;
        this.name = name;
        this.mode = mode;
        this.ignoreMissing = ignoreMissing;
    }

    public ChangeDeltaProcessor withProcessor(@NonNull TransactionProcessor processor) {
        this.processor = processor;
        return this;
    }

    public ChangeDeltaProcessor init(@NonNull ChangeDeltaProcessorConfig config,
                                     @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            LOG = LoggerFactory.getLogger(getClass());

            env = NameNodeEnv.get(name);
            Preconditions.checkNotNull(env);

            __lock = env.createLock(env.moduleInstance().getName());
            __lock.lock();
            try {
                this.processorConfig = config;
                processorConfig.read();

                sender = new HCDCMessagingBuilders.SenderBuilder()
                        .config(processorConfig.senderConfig.config())
                        .manager(manger)
                        .connection(processorConfig().senderConfig.connection())
                        .type(processorConfig().senderConfig.type())
                        .partitioner(processorConfig().senderConfig.partitionerClass())
                        .auditLogger(NameNodeEnv.get(name).auditLogger())
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
                        .auditLogger(NameNodeEnv.get(name).auditLogger())
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
            /*
            LongTxState state = (LongTxState) stateManager.processingState();
            if (txId > state.getProcessedTxId()) {
                state = (LongTxState) stateManager.update(txId);
            }
             */
            } finally {
                __lock.unlock();
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
        try {
            NameNodeEnv.get(name).agentState().state(CDCAgentState.EAgentState.Active);
            doRun();
            NameNodeEnv.get(name).agentState().state(CDCAgentState.EAgentState.Stopped);
        } catch (Throwable t) {
            try {
                NameNodeEnv.get(name).agentState().error(t);
                env.LOG.error(t.getLocalizedMessage(), t);
            } catch (Exception ex) {
                env.LOG.error(ex.getLocalizedMessage(), ex);
            }
        } finally {
            try {
                close();
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            }
        }
    }

    public LongTxState updateReadState(String messageId) throws ManagerStateError {
        LongTxState state = (LongTxState) stateManager.processingState();
        processedMessageId = state.getCurrentMessageId();

        return (LongTxState) stateManager.updateMessageId(messageId);
    }

    public void doRun() throws Throwable {
        Preconditions.checkNotNull(processor);
        Preconditions.checkNotNull(sender);
        Preconditions.checkNotNull(receiver);
        Preconditions.checkNotNull(errorSender);
        try {
            while (NameNodeEnv.get(name()).state().isAvailable()) {
                List<MessageObject<String, DFSChangeDelta>> batch
                        = receiver().nextBatch(receiveBatchTimeout);
                if (batch == null || batch.isEmpty()) {
                    Thread.sleep(receiveBatchTimeout);
                    continue;
                }
                LOG.debug(String.format("Received messages. [count=%d]", batch.size()));
                __lock.lock();
                try {
                    batchStart();
                    for (MessageObject<String, DFSChangeDelta> message : batch) {
                        int retryCount = 0;
                        while (true) {
                            try {
                                handleMessage(message);
                                break;
                            } catch (DistributedLock.LockError le) {
                                if (retryCount > LOCK_RETRY_COUNT) {
                                    throw new Exception(
                                            String.format("Error acquiring lock. [error=%s][retries=%d]",
                                                    le.getLocalizedMessage(), retryCount));
                                }
                                LOG.warn(String.format("Failed to acquire lock, will retry... [error=%s][retries=%d]",
                                        le.getLocalizedMessage(), retryCount));
                                Thread.sleep(receiveBatchTimeout);
                                retryCount++;
                            } catch (InvalidMessageError me) {
                                LOG.error(me.getLocalizedMessage());
                                errorSender.send(message);
                            }
                        }
                    }
                    batchEnd();
                } finally {
                    __lock.unlock();
                }
            }
            LOG.warn(String.format("[%s]] thread stopped.", name));
        } catch (Throwable t) {
            LOG.error(String.format("[%s]] thread terminated with error.", name), t);
            DefaultLogger.stacktrace(LOG, t);
            throw t;
        }
    }

    private void handleMessage(MessageObject<String, DFSChangeDelta> message) throws Throwable {

        BaseTxId txId = null;
        if (!isValidMessage(message)) {
            throw new InvalidMessageError(message.id(),
                    String.format("Invalid Message mode. [id=%s][mode=%s]", message.id(), message.mode().name()));
        }

        LongTxState state = updateReadState(message.id());
        boolean retry = false;
        if (!Strings.isNullOrEmpty(state.getCurrentMessageId()) &&
                !Strings.isNullOrEmpty(processedMessageId()))
            retry = state.getCurrentMessageId().compareTo(processedMessageId()) == 0;
        txId = processor.checkMessageSequence(message, ignoreMissing, retry);
        Object data = ChangeDeltaSerDe.parse(message.value(),
                Class.forName(message.value().getType()));
        try {
            DFSTransaction tnx = processor.extractTransaction(data);
            if (tnx != null) {
                LOGGER.debug(getClass(), txId.getId(),
                        String.format("PROCESSING: [TXID=%d][OP=%s]",
                                tnx.getId(), tnx.getOp().name()));
                if (tnx.getId() != txId.getId()) {
                    throw new InvalidMessageError(message.id(),
                            String.format("Transaction ID mismatch: [expected=%d][actual=%d]",
                                    txId.getId(), tnx.getId()));
                }
            }
            process(message, data, tnx, retry);
            NameNodeEnv.audit(name, getClass(), (MessageOrBuilder) data);
            if (mode == EProcessorMode.Reader) {
                commitReceived(message, txId);
            } else
                commit(message, txId);
        } catch (Exception ex) {
            error(message, data, ex, txId);
        }
    }

    public abstract boolean isValidMessage(@NonNull MessageObject<String, DFSChangeDelta> message);

    public void commitReceived(@NonNull MessageObject<String, DFSChangeDelta> message,
                               @NonNull BaseTxId txId) throws Exception {
        if (txId.getId() > 0) {
            if (message.mode() == MessageObject.MessageMode.New ||
                    message.mode() == MessageObject.MessageMode.Snapshot) {
                boolean snapshot = (message.mode() == MessageObject.MessageMode.Snapshot);
                if (stateManager().processingState().getProcessedTxId().compare(txId, snapshot) < 0) {
                    stateManager().update(txId);
                }
                if (stateManager.moduleTxState().getReceivedTxId() < txId.getId()) {
                    stateManager().updateReceivedTx(txId.getId());
                    LOGGER.info(getClass(), txId.getId(),
                            String.format("Received transaction delta. [TXID=%s]", txId.asString()));
                }
                if (message.mode() == MessageObject.MessageMode.Snapshot) {
                    if (stateManager().getModuleState().getSnapshotTxId() < txId.getId()) {
                        stateManager().updateSnapshotTx(txId.getId());
                    }
                }
            }
        }
        receiver.ack(message.id());
    }

    public void commit(@NonNull MessageObject<String, DFSChangeDelta> message,
                       @NonNull BaseTxId txId) throws Exception {
        if (txId.getId() > 0) {
            if (message.mode() == MessageObject.MessageMode.New ||
                    message.mode() == MessageObject.MessageMode.Snapshot) {
                boolean snapshot = (message.mode() == MessageObject.MessageMode.Snapshot);
                if (stateManager().processingState().getProcessedTxId().compare(txId, snapshot) < 0) {
                    stateManager().update(txId);
                }
                if (stateManager.moduleTxState().getCommittedTxId() < txId.getId()) {
                    stateManager().updateCommittedTx(txId.getId());
                    LOGGER.info(getClass(), txId.getId(),
                            String.format("Committed transaction delta. [TXID=%s]", txId.asString()));
                }
            }
        }
        receiver.ack(message.id());
    }

    public void error(@NonNull MessageObject<String, DFSChangeDelta> message,
                      @NonNull Object data,
                      @NonNull Throwable error,
                      BaseTxId txId) throws Throwable {
        if (error instanceof InvalidTransactionError) {
            LOGGER.error(getClass(), ((InvalidTransactionError) error).getTxId(), error);
            processor.handleError(message, data, (InvalidTransactionError) error);
            processor.updateTransaction(txId, message);
        } else if (error instanceof InvalidMessageError) {
            LOG.error(
                    String.format("Invalid Message: [ID=%s] [error=%s]",
                            message.id(), error.getLocalizedMessage()));
        } else {
            throw error;
        }
        errorSender.send(message);
        receiver.ack(message.id());
    }

    public abstract void batchStart() throws Exception;

    public abstract void batchEnd() throws Exception;

    public abstract void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                                 @NonNull Object data,
                                 DFSTransaction tnx,
                                 boolean retry) throws Exception;

    public abstract ChangeDeltaProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                              @NonNull ConnectionManager manger) throws ConfigurationException;

    @Override
    public void close() throws IOException {
        if (sender != null) {
            sender.close();
            sender = null;
        }
        if (receiver != null) {
            receiver.close();
            receiver = null;
        }
        if (errorSender != null) {
            errorSender.close();
            errorSender = null;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ChangeDeltaProcessorConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "processor.source";

        public static class Constants {
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
            super(config, __CONFIG_PATH);
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
