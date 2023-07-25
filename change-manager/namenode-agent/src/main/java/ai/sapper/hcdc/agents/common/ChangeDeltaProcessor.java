/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.InvalidTransactionError;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.executor.ETaskState;
import ai.sapper.cdc.core.executor.FatalError;
import ai.sapper.cdc.core.executor.TaskResponse;
import ai.sapper.cdc.core.messaging.*;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.model.*;
import ai.sapper.cdc.core.processing.*;
import ai.sapper.cdc.core.state.HCdcStateManager;
import ai.sapper.cdc.core.utils.Timer;
import ai.sapper.hcdc.agents.settings.ChangeDeltaProcessorSettings;
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
import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public abstract class ChangeDeltaProcessor<MO extends ReceiverOffset>
        extends BatchMessageProcessor<HCdcTxId, String, DFSChangeDelta, EHCdcProcessorState, HCdcTxId, MO> {
    public enum EProcessorMode {
        Reader, Committer
    }

    public static final int LOCK_RETRY_COUNT = 16;

    private MessageSender<String, DFSChangeDelta> sender;
    private long receiveBatchTimeout = 1000;
    private final NameNodeEnv env;
    protected TransactionProcessor processor;
    protected final EProcessorMode mode;
    protected final boolean ignoreMissing;
    private final Class<? extends ChangeDeltaProcessorSettings> settingsType;
    protected String name;
    protected ChangeDeltaProcessorSettings settings;


    public ChangeDeltaProcessor(@NonNull NameNodeEnv env,
                                @NonNull Class<? extends ChangeDeltaProcessorSettings> settingsType,
                                @NonNull EProcessorMode mode,
                                @NonNull HCdcBaseMetrics metrics,
                                boolean ignoreMissing) {
        super(env, metrics, HCdcProcessingState.class);
        Preconditions.checkState(super.stateManager() instanceof HCdcStateManager);
        Preconditions.checkState(super.stateManager().processingState() instanceof HCdcMessageProcessingState);
        this.mode = mode;
        this.ignoreMissing = ignoreMissing;
        this.settingsType = settingsType;
        this.env = env;
    }

    @Override
    public ChangeDeltaProcessor<MO> init(@NonNull String name,
                                         @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         String path) throws ConfigurationException {
        if (Strings.isNullOrEmpty(path)) {
            path = ChangeDeltaProcessorSettings.__CONFIG_PATH;
        }
        receiverConfig = new ChangeDeltaProcessorConfig(xmlConfig, path, settingsType);
        super.init(name, xmlConfig, path);
        settings = (ChangeDeltaProcessorSettings) receiverConfig.settings();
        return this;
    }

    @Override
    protected void postInit(@NonNull MessagingProcessorSettings settings) throws Exception {
        sender = ((ChangeDeltaProcessorConfig) receiverConfig).readSender(env);
        receiveBatchTimeout = settings.getReceiveBatchTimeout();
        if (processingState().getOffset() == null) {
            processingState().setOffset(new HCdcTxId(-1));
        }
    }

    @Override
    protected void process(@NonNull List<MessageObject<String, DFSChangeDelta>> messages,
                           @NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> processorState,
                           @NonNull List<TaskResponse<HCdcTxId>> responses) throws Exception {
        int ii = 0;
        for (MessageObject<String, DFSChangeDelta> message : messages) {
            TaskResponse<HCdcTxId> response = responses.get(ii);
            Preconditions.checkState(response instanceof HCdcTaskResponse);
            Preconditions.checkState(((HCdcTaskResponse) response).message().id().equals(message.id()));
            response.state(ETaskState.RUNNING);
            try (Timer t = new Timer(metrics.getTimer(EventProcessorMetrics.METRIC_EVENTS_TIME))) {
                handle(message, processorState, (HCdcTaskResponse) response);
                response.state(ETaskState.DONE);
                metrics.getCounter(EventProcessorMetrics.METRIC_EVENTS_PROCESSED).increment();
            } catch (Exception ex) {
                metrics.getCounter(EventProcessorMetrics.METRIC_EVENTS_ERROR).increment();
                response.markError(ex);
                throw ex;
            }
        }
    }

    @Override
    protected MessageTaskResponse<HCdcTxId, String, DFSChangeDelta> initResponse(@NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> processorState,
                                                                                 @NonNull MessageObject<String, DFSChangeDelta> message) {
        HCdcTaskResponse response = new HCdcTaskResponse();
        response.message(message);
        response.error(null);
        response.state(ETaskState.INITIALIZED);
        return response;
    }

    private void handle(@NonNull MessageObject<String, DFSChangeDelta> message,
                        @NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> processorState,
                        @NonNull HCdcTaskResponse response) throws Exception {
        HCdcTxId txId = null;
        if (!isValidMessage(message)) {
            throw new InvalidMessageError(message.id(),
                    String.format("Invalid Message mode. [id=%s][mode=%s]", message.id(), message.mode().name()));
        }
        HCdcStateManager stateManager = (HCdcStateManager) stateManager();
        HCdcMessageProcessingState<MO> pState = (HCdcMessageProcessingState<MO>) processorState;
        boolean retry = pState.isLastProcessedMessage(message.id());
        txId = processor.checkMessageSequence(message, ignoreMissing, retry);
        Object data = ChangeDeltaSerDe.parse(message.value(),
                Class.forName(message.value().getType()));
        response.data(data);
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
            Params params = new Params();
            params.dfsTx(tnx);
            params.retry(retry);
            process(message, data, pState, params, response);
            NameNodeEnv.audit(name(), getClass(), (MessageOrBuilder) data);
            if (mode == EProcessorMode.Reader) {
                commitReceived(message, txId, pState);
            } else
                commit(message, txId, pState);
        } catch (Exception ex) {
            try {
                error(message, data, ex, response, txId);
            } catch (Throwable t) {
                throw new FatalError(t);
            }
        } finally {
            pState.setLastMessageId(message.id());
            if (pState.getSnapshotOffset() != null)
                stateManager.updateSnapshotOffset(pState.getSnapshotOffset());
        }
    }

    public abstract boolean isValidMessage(@NonNull MessageObject<String, DFSChangeDelta> message);

    public void commitReceived(@NonNull MessageObject<String, DFSChangeDelta> message,
                               @NonNull HCdcTxId txId,
                               @NonNull HCdcMessageProcessingState<MO> pState) throws Exception {
        if (txId.getId() > 0) {
            if (message.mode() == MessageObject.MessageMode.New ||
                    message.mode() == MessageObject.MessageMode.Snapshot) {
                if (pState.getReceivedTx() != null) {
                    if (pState.getReceivedTx().compare(txId) < 0) {
                        pState.setOffset(txId);
                    }
                } else {
                    pState.setReceivedTx(txId);
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
                if (pState.getOffset().compare(txId) < 0) {
                    pState.updateProcessedTxId(txId.getId());
                }
                if (snapshot) {
                    SnapshotOffset offset = pState.getSnapshotOffset();
                    if (offset == null) {
                        offset = new SnapshotOffset();
                        pState.setSnapshotOffset(offset);
                    }
                    if (offset.getSnapshotTxId() < txId.getId()) {
                        pState.updateSnapshotTxId(txId.getId());
                    } else if (offset.getSnapshotSeq() < txId.getSequence()) {
                        pState.updateSnapshotSequence(txId.getId(), txId.getSequence());
                    }
                }
            }
        }
        receiver.ack(message.id());
    }

    @Override
    protected void checkError(TaskResponse<HCdcTxId> taskResponse) throws Exception {
        Preconditions.checkArgument(taskResponse instanceof HCdcTaskResponse);
        HCdcTaskResponse response
                = (HCdcTaskResponse) taskResponse;
        if (response.hasError()) {
            if (response.error() instanceof InvalidTransactionError ||
                    response.error() instanceof InvalidTransactionError) {
                return;
            } else {
                throw new FatalError(response.error());
            }
        }
    }

    public void error(@NonNull MessageObject<String, DFSChangeDelta> message,
                      @NonNull Object data,
                      @NonNull Throwable error,
                      @NonNull HCdcTaskResponse response,
                      HCdcTxId txId) throws Throwable {
        if (error instanceof InvalidTransactionError) {
            LOGGER.error(getClass(), ((InvalidTransactionError) error).getTxId(), error);
            processor.handleError(message, data, (InvalidTransactionError) error);
            processor.updateTransaction(txId, message);
            response.markError(error);
        } else if (error instanceof InvalidMessageError) {
            LOG.error(
                    String.format("Invalid Message: [ID=%s] [error=%s]",
                            message.id(), error.getLocalizedMessage()));
            response.markError(error);
        } else {
            response.markError(error);
            throw error;
        }
        errorLogger.send(message);
        receiver.ack(message.id());
    }

    public abstract void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                                 @NonNull Object data,
                                 @NonNull HCdcMessageProcessingState<MO> pState,
                                 @NonNull Params params,
                                 @NonNull HCdcTaskResponse response) throws Exception;

    public abstract ChangeDeltaProcessor<MO> init(@NonNull String name,
                                                  @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException;

    @Override
    protected ProcessingState<EHCdcProcessorState, HCdcTxId> finished(@NonNull ProcessingState<EHCdcProcessorState, HCdcTxId> processingState) {
        processingState.setState(EHCdcProcessorState.Stopped);
        return processingState;
    }

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
    public static class ProcessorDef {
        private Class<? extends ChangeDeltaProcessor<?>> type;
        private String name;
    }

    @SuppressWarnings("unchecked")
    public static ProcessorDef readProcessorType(
            @NonNull HierarchicalConfiguration<ImmutableNode> config) throws Exception {
        HierarchicalConfiguration<ImmutableNode> node = config.configurationAt(ChangeDeltaProcessorSettings.__CONFIG_PATH);
        if (node != null) {
            ProcessorDef def = new ProcessorDef();
            String cname = node.getString(ChangeDeltaProcessorSettings.__CONFIG_PROCESSOR_TYPE);
            if (!Strings.isNullOrEmpty(cname)) {
                def.type = (Class<? extends ChangeDeltaProcessor<?>>) Class.forName(cname);
            }
            def.name = node.getString(ChangeDeltaProcessorSettings.__CONFIG_PROCESSOR_NAME);
            return def;
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
                    .getDeclaredConstructor()
                    .newInstance();
            HierarchicalConfiguration<ImmutableNode> eConfig
                    = config().configurationAt(ChangeDeltaProcessorSettings.__CONFIG_PATH_SENDER);
            if (eConfig == null) {
                throw new ConfigurationException(
                        String.format("Sender queue configuration not found. [path=%s]",
                                ChangeDeltaProcessorSettings.__CONFIG_PATH_SENDER));
            }
            return builder.withEnv(env).build(eConfig);
        }
    }
}
