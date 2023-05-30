package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.messaging.*;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.model.BaseAgentState;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.processing.MessageProcessor;
import ai.sapper.cdc.core.processing.Processor;
import ai.sapper.cdc.core.state.StateManagerError;
import ai.sapper.hcdc.agents.model.EHCdcProcessorState;
import ai.sapper.hcdc.agents.settings.ChangeDeltaProcessorSettings;
import ai.sapper.hcdc.agents.settings.HDFSSnapshotProcessorSettings;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSTransaction;
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
public abstract class ChangeDeltaProcessor extends MessageProcessor<String, DFSChangeDelta, EHCdcProcessorState, HCdcTxId> {
    public enum EProcessorMode {
        Reader, Committer
    }

    public static final int LOCK_RETRY_COUNT = 16;

    private static Logger LOG;

    private MessageSender<String, DFSChangeDelta> sender;
    private long receiveBatchTimeout = 1000;
    private NameNodeEnv env;
    private String processedMessageId = null;
    private TransactionProcessor processor;
    private final EProcessorMode mode;
    private final boolean ignoreMissing;
    private final Class<? extends ChangeDeltaProcessorSettings> settingsType;

    public ChangeDeltaProcessor(@NonNull NameNodeEnv env,
                                @NonNull Class<? extends ChangeDeltaProcessorSettings> settingsType,
                                @NonNull EProcessorMode mode,
                                boolean ignoreMissing) {
        super(env, HCdcProcessingState.class);
        Preconditions.checkState(super.stateManager() instanceof HCdcStateManager);
        this.mode = mode;
        this.ignoreMissing = ignoreMissing;
        this.settingsType = settingsType;
    }

    public ChangeDeltaProcessor withProcessor(@NonNull TransactionProcessor processor) {
        this.processor = processor;
        return this;
    }

    @Override
    public Processor<EHCdcProcessorState, HCdcTxId> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                         String path) throws ConfigurationException {
        if (Strings.isNullOrEmpty(path)) {
            path = ChangeDeltaProcessorSettings.__CONFIG_PATH;
        }
        receiverConfig = new ChangeDeltaProcessorConfig(xmlConfig, path, settingsType);
        return super.init(xmlConfig, path);
    }

    @Override
    protected void postInit(@NonNull MessagingProcessorSettings settings) throws Exception {
        sender = ((ChangeDeltaProcessorConfig) receiverConfig).readSender(env);
        receiveBatchTimeout = ((ChangeDeltaProcessorSettings) settings).getReceiveBatchTimeout();
    }


    public BaseTxState updateReadState(String messageId) throws StateManagerError {
        BaseTxState state = (BaseTxState) stateManager.processingState();
        processedMessageId = state.getCurrentMessageId();

        return (LongTxState) stateManager.updateMessageId(messageId);
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
                       @NonNull HCdcTxId txId) throws Exception {
        if (txId.getId() > 0) {
            if (message.mode() == MessageObject.MessageMode.New ||
                    message.mode() == MessageObject.MessageMode.Snapshot) {
                boolean snapshot = (message.mode() == MessageObject.MessageMode.Snapshot);
                if (stateManager().processingState().getProcessedOffset().compare(txId, snapshot) < 0) {
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
                      HCdcTxId txId) throws Throwable {
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
        errorLogger.send(message);
        receiver.ack(message.id());
    }

    public abstract void batchStart() throws Exception;

    public abstract void batchEnd() throws Exception;

    public abstract void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                                 @NonNull Object data,
                                 DFSTransaction tnx,
                                 boolean retry) throws Exception;

    public abstract ChangeDeltaProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    @Override
    public void close() throws IOException {
        if (sender != null) {
            sender.close();
            sender = null;
        }
        super.close();
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ChangeDeltaProcessorConfig extends MessagingProcessorConfig {

        public ChangeDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                          @NonNull Class<? extends ChangeDeltaProcessorSettings> settingsType) {
            super(config, ChangeDeltaProcessorSettings.__CONFIG_PATH, settingsType);
        }

        public ChangeDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                          @NonNull String path,
                                          @NonNull Class<? extends ChangeDeltaProcessorSettings> settingsType) {
            super(config, path, settingsType);
        }

        @SuppressWarnings("unchecked")
        public MessageSender<String, DFSChangeDelta> readSender(@NonNull BaseEnv<?> env) throws Exception {
            ChangeDeltaProcessorSettings settings = (ChangeDeltaProcessorSettings) settings();
            MessageSenderBuilder<String, DFSChangeDelta> builder
                    = (MessageSenderBuilder<String, DFSChangeDelta>) settings.getBuilderType()
                    .getDeclaredConstructor(BaseEnv.class, Class.class)
                    .newInstance(env, settings.getBuilderSettingsType());
            HierarchicalConfiguration<ImmutableNode> eConfig
                    = config().configurationAt(HDFSSnapshotProcessorSettings.__CONFIG_PATH_SENDER);
            if (eConfig == null) {
                throw new ConfigurationException(
                        String.format("Sender queue configuration not found. [path=%s]",
                                HDFSSnapshotProcessorSettings.__CONFIG_PATH_SENDER));
            }
            return builder.build(eConfig);
        }
    }
}
