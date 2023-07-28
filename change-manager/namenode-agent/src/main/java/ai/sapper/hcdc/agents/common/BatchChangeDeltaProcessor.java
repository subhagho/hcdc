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

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.executor.*;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.InvalidMessageError;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.ReceiverOffset;
import ai.sapper.cdc.core.model.*;
import ai.sapper.cdc.core.processing.EventProcessorMetrics;
import ai.sapper.cdc.core.processing.MessageProcessorState;
import ai.sapper.cdc.core.state.BaseStateManager;
import ai.sapper.cdc.core.state.HCdcStateManager;
import ai.sapper.cdc.core.utils.Timer;
import ai.sapper.cdc.entity.executor.EntityTask;
import ai.sapper.cdc.entity.manager.SchemaManager;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.hcdc.agents.settings.ChangeDeltaProcessorSettings;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSTransaction;
import com.google.common.base.Preconditions;
import com.google.protobuf.MessageOrBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

public abstract class BatchChangeDeltaProcessor<MO extends ReceiverOffset> extends ChangeDeltaProcessor<MO> {
    private HCdcShardedExecutor executor;

    public BatchChangeDeltaProcessor(@NonNull NameNodeEnv env,
                                     @NonNull Class<? extends ChangeDeltaProcessorSettings> settingsType,
                                     @NonNull EProcessorMode mode,
                                     @NonNull HCdcBaseMetrics metrics,
                                     boolean ignoreMissing) {
        super(env, settingsType, mode, metrics, ignoreMissing);
    }

    @Override
    public ChangeDeltaProcessor<MO> init(@NonNull String name,
                                         @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                         String path) throws ConfigurationException {
        super.init(name, xmlConfig, path);
        executor = new HCdcShardedExecutor();
        executor.init(receiverConfig.config(), env());
        return this;
    }

    @Override
    protected void process(@NonNull List<MessageObject<String, DFSChangeDelta>> messages,
                           @NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> processorState,
                           @NonNull List<TaskResponse<HCdcTxId>> responses) throws Exception {
        Preconditions.checkNotNull(processor);
        int ii = 0;
        for (MessageObject<String, DFSChangeDelta> message : messages) {
            TaskResponse<HCdcTxId> response = responses.get(ii);
            Preconditions.checkState(response instanceof HCdcTaskResponse);
            Preconditions.checkState(((HCdcTaskResponse) response).message().id().equals(message.id()));
            response.state(ETaskState.RUNNING);
            try (Timer t = new Timer(metrics.getTimer(EventProcessorMetrics.METRIC_EVENTS_TIME))) {
                submit(message, processorState, (HCdcTaskResponse) response);
            } catch (Exception ex) {
                metrics.getCounter(EventProcessorMetrics.METRIC_EVENTS_ERROR).increment();
                response.markError(ex);
                throw ex;
            }
        }
    }

    protected void submit(@NonNull MessageObject<String, DFSChangeDelta> message,
                          @NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> processorState,
                          @NonNull HCdcTaskResponse response) throws Exception {
        try {
            HCdcTxId txId = null;
            if (!isValidMessage(message)) {
                throw new InvalidMessageError(message.id(),
                        String.format("Invalid Message mode. [id=%s][mode=%s]", message.id(), message.mode().name()));
            }
            HCdcMessageProcessingState<MO> pState = (HCdcMessageProcessingState<MO>) processorState;
            boolean retry = pState.isLastProcessedMessage(message.id());
            txId = processor.checkMessageSequence(message, ignoreMissing, retry);
            Object data = ChangeDeltaSerDe.parse(message.value(),
                    Class.forName(message.value().getType()));
            response.data(data);
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
            SchemaEntity entity = processor.extract(message, data);
            TaskParams<MO> params = new TaskParams<>();
            params.dfsTx(tnx);
            params.retry(retry);
            params.response(response);
            params.state(pState);
            params.entity(entity);
            params.message(message);
            params.data(data);

            ChangeDeltaTask<MO> task = new ChangeDeltaTask<>(stateManager(),
                    env().schemaManager(),
                    getClass().getSimpleName(),
                    this,
                    params);
            executor.submit(task);
        } catch (Exception ex) {
            DefaultLogger.error(ex.getLocalizedMessage());
            DefaultLogger.stacktrace(ex);
            response.markError(ex);
            throw ex;
        }
    }

    protected void handle(@NonNull MessageObject<String, DFSChangeDelta> message,
                          @NonNull Object data,
                          @NonNull Params params,
                          @NonNull HCdcMessageProcessingState<MO> pState,
                          @NonNull HCdcTxId txId,
                          @NonNull HCdcTaskResponse response) throws Exception {
        HCdcStateManager stateManager = (HCdcStateManager) stateManager();
        try {
            process(message, data, pState, params, response);
            NameNodeEnv.audit(name(), getClass(), (MessageOrBuilder) data);
            if (mode == EProcessorMode.Reader) {
                commitReceived(message, txId, pState);
            } else
                commit(message, txId, pState);
            response.state(ETaskState.DONE);
            metrics.getCounter(EventProcessorMetrics.METRIC_EVENTS_PROCESSED).increment();
        } catch (Exception ex) {
            metrics.getCounter(EventProcessorMetrics.METRIC_EVENTS_ERROR).increment();
            response.markError(ex);
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

    @Getter
    @Accessors(fluent = true)
    public static class ChangeDeltaTask<MO extends ReceiverOffset> extends EntityTask<HCdcTxId> {
        private final BatchChangeDeltaProcessor<MO> processor;
        private final MessageObject<String, DFSChangeDelta> message;
        private final Object data;
        private final Params params;
        private final HCdcMessageProcessingState<MO> pState;
        private final HCdcTxId txId;

        public ChangeDeltaTask(@NonNull BaseStateManager stateManager,
                               @NonNull SchemaManager schemaManager,
                               @NonNull String type,
                               @NonNull BatchChangeDeltaProcessor<MO> processor,
                               @NonNull TaskParams<MO> params) {
            super(stateManager, schemaManager, type, params.entity());
            this.processor = processor;
            this.message = params.message();
            this.data = params.data();
            this.params = params;
            this.pState = params.state();
            this.txId = params.txId();
            withResponse(params.response());
        }

        public ChangeDeltaTask(@NonNull BaseStateManager stateManager,
                               @NonNull SchemaManager schemaManager,
                               @NonNull BatchChangeDeltaProcessor<MO> processor,
                               @NonNull TaskParams<MO> params) {
            super(stateManager, schemaManager, params.entity());
            this.processor = processor;
            this.message = params.message();
            this.data = params.data();
            this.params = params;
            this.pState = params.state();
            this.txId = params.txId();
            withResponse(params.response());
        }

        @Override
        public TaskBatchResponse<HCdcTxId> initResponse() {
            TaskBatchResponse<HCdcTxId> response = new TaskBatchResponse<>();
            response.state(ETaskState.INITIALIZED);
            return response;
        }

        @Override
        public HCdcTxId execute() throws Exception {
            try {
                DefaultLogger.debug(String.format("Executing task: [id=%s][entity=%s]", id(), entity().toString()));
                processor.handle(message, data, params, pState, txId, (HCdcTaskResponse) response());
                response().state(ETaskState.DONE);
                response().result(txId);
                return txId;
            } catch (Throwable t) {
                DefaultLogger.error(t.getLocalizedMessage());
                DefaultLogger.stacktrace(t);
                response().markError(t);
                throw new Exception(t);
            }
        }

        @Override
        public void close() throws IOException {

        }
    }
}
