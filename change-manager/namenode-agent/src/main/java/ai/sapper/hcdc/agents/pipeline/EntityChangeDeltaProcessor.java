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

package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.ReceiverOffset;
import ai.sapper.cdc.core.model.*;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.core.processing.MessageProcessorState;
import ai.sapper.cdc.core.processing.ProcessingState;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.core.state.HCdcStateManager;
import ai.sapper.cdc.core.utils.ProtoUtils;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import ai.sapper.hcdc.agents.common.BatchChangeDeltaProcessor;
import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.settings.EntityChangeDeltaProcessorSettings;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Getter
@Accessors(fluent = true)
public class EntityChangeDeltaProcessor<MO extends ReceiverOffset> extends BatchChangeDeltaProcessor<MO> {
    private static Logger LOG = LoggerFactory.getLogger(EntityChangeDeltaProcessor.class.getCanonicalName());
    private final HCdcSchemaManager schemaManager;


    public EntityChangeDeltaProcessor(@NonNull NameNodeEnv env,
                                      @NonNull String name) {
        super(env,
                EntityChangeDeltaProcessorSettings.class,
                EProcessorMode.Reader,
                new EntityChangeDeltaMetrics(env.name(), env),
                true);
        schemaManager = env.schemaManager();
        this.name = name;
    }

    public void cleanFileState() throws Exception {
        HCdcStateManager stateManager = (HCdcStateManager) stateManager();
        List<DFSFileState> files = stateManager.fileStateHelper().listFiles(null, EFileState.Deleted);
        if (files != null && !files.isEmpty()) {
            for (DFSFileState file : files) {
                DFSFileState f = stateManager.fileStateHelper().delete(file.getFileInfo().getHdfsPath());
                if (f != null) {
                    LOG.debug(String.format("File node deleted. [path=%s]", f.getFileInfo().getHdfsPath()));
                } else {
                    LOG.error(String.format("Failed to delete file node. [path=%s]", file.getFileInfo().getHdfsPath()));
                }
            }
        }
    }

    public boolean isValidMessage(@NonNull MessageObject<String, DFSChangeDelta> message) {
        boolean ret = false;
        if (message.mode() != null) {
            ret = (message.mode() == MessageObject.MessageMode.New
                    || message.mode() == MessageObject.MessageMode.Backlog
                    || message.mode() == MessageObject.MessageMode.Snapshot
                    || message.mode() == MessageObject.MessageMode.Forked
                    || message.mode() == MessageObject.MessageMode.Recursive);
        }
        if (ret) {
            ret = message.value().hasTx();
        }
        return ret;
    }

    @Override
    public void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                        @NonNull Object data,
                        @NonNull HCdcMessageProcessingState<MO> pState,
                        @NonNull Params params,
                        @NonNull HCdcTaskResponse response) throws Exception {
        try {
            HCdcTxId txId = null;
            if (params.dfsTx() != null) {
                txId = ProtoUtils.fromTx(params.dfsTx());
            } else {
                txId = new HCdcTxId(-1);
            }
            params.txId(txId);
            EntityChangeTransactionProcessor processor
                    = (EntityChangeTransactionProcessor) processor();
            processor.processTxMessage(message, data, params);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            response.markError(ex);
            throw ex;
        }
    }

    @Override
    public ChangeDeltaProcessor<MO> init(@NonNull String name,
                                         @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        super.init(name, xmlConfig, EntityChangeDeltaProcessorSettings.__CONFIG_PATH);
        processor
                = new EntityChangeTransactionProcessor(name(), env(), (HCdcBaseMetrics) metrics)
                .withSenderQueue(sender())
                .withErrorQueue(errorLogger);
        state.setState(ProcessorState.EProcessorState.Initialized);
        return this;
    }

    @Override
    protected void initState(@NonNull ProcessingState<EHCdcProcessorState, HCdcTxId> processingState) throws Exception {

    }

    @Override
    protected void batchStart(@NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> messageProcessorState) throws Exception {

    }

    @Override
    protected void batchEnd(@NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> messageProcessorState) throws Exception {

    }

    public static class EntityChangeDeltaMetrics extends HCdcBaseMetrics {
        public EntityChangeDeltaMetrics(@NonNull String name,
                                        @NonNull BaseEnv<?> env) {
            super(name, env, EntityChangeDeltaProcessor.class.getSimpleName());
        }
    }
}
