package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.InvalidTransactionError;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.*;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.model.EHCdcProcessorState;
import ai.sapper.cdc.core.model.HCdcMessageProcessingState;
import ai.sapper.cdc.core.model.HCdcProcessingState;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.processing.MessageProcessor;
import ai.sapper.cdc.core.processing.MessageProcessorState;
import ai.sapper.cdc.core.state.HCdcStateManager;
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

import java.io.IOException;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public abstract class ChangeDeltaProcessor<MO extends ReceiverOffset>
        extends MessageProcessor<String, DFSChangeDelta, EHCdcProcessorState, HCdcTxId, MO> {
    public enum EProcessorMode {
        Reader, Committer
    }

    public static final int LOCK_RETRY_COUNT = 16;

    private MessageSender<String, DFSChangeDelta> sender;
    private long receiveBatchTimeout = 1000;
    private NameNodeEnv env;
    private String processedMessageId = null;
    private TransactionProcessor processor;
    private final EProcessorMode mode;
    private final boolean ignoreMissing;
    private final Class<? extends ChangeDeltaProcessorSettings> settingsType;
    protected String name;

    public ChangeDeltaProcessor(@NonNull NameNodeEnv env,
                                @NonNull Class<? extends ChangeDeltaProcessorSettings> settingsType,
                                @NonNull EProcessorMode mode,
                                boolean ignoreMissing) {
        super(env, HCdcProcessingState.class);
        Preconditions.checkState(super.stateManager() instanceof HCdcStateManager);
        Preconditions.checkState(super.stateManager().processingState() instanceof HCdcMessageProcessingState);
        this.mode = mode;
        this.ignoreMissing = ignoreMissing;
        this.settingsType = settingsType;
    }

    public ChangeDeltaProcessor<MO> withProcessor(@NonNull TransactionProcessor processor) {
        this.processor = processor;
        return this;
    }

    @Override
    public ChangeDeltaProcessor<MO> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         String path) throws ConfigurationException {
        if (Strings.isNullOrEmpty(path)) {
            path = ChangeDeltaProcessorSettings.__CONFIG_PATH;
        }
        receiverConfig = new ChangeDeltaProcessorConfig(xmlConfig, path, settingsType);
        return (ChangeDeltaProcessor<MO>) super.init(xmlConfig, path);
    }

    @Override
    protected void postInit(@NonNull MessagingProcessorSettings settings) throws Exception {
        sender = ((ChangeDeltaProcessorConfig) receiverConfig).readSender(env);
        receiveBatchTimeout = ((ChangeDeltaProcessorSettings) settings).getReceiveBatchTimeout();
    }

    @Override
    protected void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                           @NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> processorState) throws Exception {
        HCdcTxId txId = null;
        if (!isValidMessage(message)) {
            throw new InvalidMessageError(message.id(),
                    String.format("Invalid Message mode. [id=%s][mode=%s]", message.id(), message.mode().name()));
        }
        HCdcMessageProcessingState<MO> pState = (HCdcMessageProcessingState<MO>) processorState;
        boolean retry = pState.isLastProcessedMessage(processedMessageId);
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
            process(message, data, pState, tnx, retry);
            NameNodeEnv.audit(name(), getClass(), (MessageOrBuilder) data);
            if (mode == EProcessorMode.Reader) {
                commitReceived(message, txId, pState);
            } else
                commit(message, txId, pState);
        } catch (Exception ex) {
            error(message, data, ex, txId);
        }
    }

    public abstract boolean isValidMessage(@NonNull MessageObject<String, DFSChangeDelta> message);

    public void commitReceived(@NonNull MessageObject<String, DFSChangeDelta> message,
                               @NonNull HCdcTxId txId,
                               @NonNull HCdcMessageProcessingState<MO> pState) throws Exception {
        if (txId.getId() > 0) {
            if (message.mode() == MessageObject.MessageMode.New ||
                    message.mode() == MessageObject.MessageMode.Snapshot) {
                boolean snapshot = (message.mode() == MessageObject.MessageMode.Snapshot);
                if (pState.getReceivedTx().compare(txId) < 0) {
                    pState.setProcessedOffset(txId);
                }
            }
        }
        receiver.ack(message.id());
    }

    public void commit(@NonNull MessageObject<String, DFSChangeDelta> message,
                       @NonNull HCdcTxId txId,
                       @NonNull HCdcMessageProcessingState<MO> pState) throws Exception {
        if (txId.getId() > 0) {
            if (message.mode() == MessageObject.MessageMode.New ||
                    message.mode() == MessageObject.MessageMode.Snapshot) {
                boolean snapshot = (message.mode() == MessageObject.MessageMode.Snapshot);
                if (pState.getProcessedOffset().compare(txId) < 0) {
                    pState.updateProcessedTxId(txId.getId());
                }
                if (snapshot) {
                    if (pState.getSnapshotOffset().getSnapshotTxId() < txId.getId()) {
                        pState.updateSnapshotTxId(txId.getId());
                    }
                }
            }
        }
        receiver.ack(message.id());
    }

    public void error(@NonNull MessageObject<String, DFSChangeDelta> message,
                      @NonNull Object data,
                      @NonNull Exception error,
                      HCdcTxId txId) throws Exception {
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

    public abstract void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                                 @NonNull Object data,
                                 @NonNull HCdcMessageProcessingState<MO> pState,
                                 DFSTransaction tnx,
                                 boolean retry) throws Exception;

    public abstract ChangeDeltaProcessor<MO> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    @Override
    public void close() throws IOException {
        if (sender != null) {
            sender.close();
            sender = null;
        }
        super.close();
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends ChangeDeltaProcessor<?>> readProcessorType(
            @NonNull HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        HierarchicalConfiguration<ImmutableNode> node = config.configurationAt(ChangeDeltaProcessorSettings.__CONFIG_PATH);
        if (node != null) {
            String cname = node.getString(ChangeDeltaProcessorSettings.__CONFIG_PROCESSOR_TYPE);
            if (!Strings.isNullOrEmpty(cname)) {
                return (Class<? extends ChangeDeltaProcessor<?>>) Class.forName(cname);
            }
        }
        return null;
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class ChangeDeltaProcessorConfig extends MessagingProcessorConfig {

        public ChangeDeltaProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, ChangeDeltaProcessorSettings.__CONFIG_PATH, ChangeDeltaProcessorSettings.class);
        }

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
                    = (MessageSenderBuilder<String, DFSChangeDelta>) settings.getSendBuilderType()
                    .getDeclaredConstructor(BaseEnv.class, Class.class)
                    .newInstance(env, settings.getSendBuilderSettingsType());
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
