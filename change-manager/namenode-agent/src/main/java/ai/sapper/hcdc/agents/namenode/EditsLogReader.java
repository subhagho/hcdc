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

package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DFSEditsFileFinder;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.model.EHCdcProcessorState;
import ai.sapper.cdc.core.model.HCdcProcessingState;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.model.SnapshotOffset;
import ai.sapper.cdc.core.model.dfs.DFSEditLogBatch;
import ai.sapper.cdc.core.model.dfs.DFSTransactionType;
import ai.sapper.cdc.core.processing.ProcessingState;
import ai.sapper.cdc.core.processing.Processor;
import ai.sapper.cdc.core.state.HCdcStateManager;
import ai.sapper.hcdc.agents.settings.HDFSEditsReaderSettings;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.EditsLogFileReader;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public class EditsLogReader extends HDFSEditsReader {
    private static final String PATH_NN_CURRENT_DIR = "current";

    private File editsDir;
    private HierarchicalConfiguration<ImmutableNode> config;
    private HCdcTxId txId;

    public EditsLogReader(@NonNull NameNodeEnv env) {
        super(env);
    }

    @Override
    public Processor<EHCdcProcessorState, HCdcTxId> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                         String path) throws ConfigurationException {
        if (Strings.isNullOrEmpty(path)) {
            path = HDFSEditsReaderSettings.__CONFIG_PATH;
        }
        try {
            NameNodeEnv env = (NameNodeEnv) this.env;
            config = xmlConfig.configurationAt(path);
            Preconditions.checkNotNull(env.hadoopConfig());
            String dir = env.hadoopConfig().nameNodeEditsDir();
            editsDir = new File(dir);
            if (!editsDir.exists()) {
                throw new ConfigurationException(
                        String.format("Invalid Hadoop Configuration: Edits directory not found. [path=%s]",
                                editsDir.getAbsolutePath()));
            }
            setup(config, HDFSEditsReaderSettings.class);
            return this;
        } catch (Throwable ex) {
            try {
                updateError(ex);
            } catch (Throwable t) {
                DefaultLogger.stacktrace(t);
                DefaultLogger.error(LOG, "Failed to save state...", t);
            }
            throw new ConfigurationException(ex);
        }
    }

    @Override
    protected void initState(@NonNull ProcessingState<EHCdcProcessorState, HCdcTxId> processingState) throws Exception {
        HCdcStateManager stateManager = (HCdcStateManager) stateManager();
        if (processingState.getOffset() == null) {
            SnapshotOffset offset = stateManager.getSnapshotOffset();
            if (offset == null) {
                processingState.setOffset(new HCdcTxId(-1));
            } else {
                HCdcTxId txId = new HCdcTxId(offset.getSnapshotTxId());
                txId.setSequence(offset.getSnapshotSeq());
                processingState.setOffset(txId);
            }
        }
    }

    @Override
    protected ProcessingState<EHCdcProcessorState, HCdcTxId> finished(@NonNull ProcessingState<EHCdcProcessorState, HCdcTxId> processingState) {
        processingState.setState(EHCdcProcessorState.Stopped);
        return processingState;
    }

    @Override
    public void doRun(boolean runOnce) throws Exception {
        __lock().lock();
        try {
            NameNodeEnv env = (NameNodeEnv) this.env;
            HCdcProcessingState pState = (HCdcProcessingState) processingState();
            EditsLogFileReader reader = new EditsLogFileReader();
            txId = pState.getOffset();
            if (txId.getId() < 0) {
                LOGGER.warn(String.format("Name Node replication not initialized. [source=%s]",
                        env.source()));
            }
            List<DFSEditsFileFinder.EditsLogFile> files = DFSEditsFileFinder
                    .findEditsFiles(getPathNnCurrentDir(editsDir.getAbsolutePath()),
                            txId.getId() + 1, -1);
            if (files != null && !files.isEmpty()) {
                for (DFSEditsFileFinder.EditsLogFile file : files) {
                    LOGGER.debug(getClass(), txId,
                            String.format("Reading edits file [path=%s][startTx=%d]",
                                    file, txId.getId()));

                    reader.run(file,
                            txId.getId(),
                            file.endTxId(),
                            env);
                    DFSEditLogBatch batch = reader.batch();
                    if (batch.transactions() != null && !batch.transactions().isEmpty()) {
                        processBatch(batch, txId, env.source());
                        pState = (HCdcProcessingState) updateState(txId);
                    }
                }
            }
            String cf = DFSEditsFileFinder.getCurrentEditsFile(getPathNnCurrentDir(editsDir.getAbsolutePath()));
            if (cf == null) {
                throw new Exception(String.format("Current Edits file not found. [dir=%s]",
                        editsDir.getAbsolutePath()));
            }
            long ltx = DFSEditsFileFinder.findSeenTxID(getPathNnCurrentDir(editsDir.getAbsolutePath()));
            LOGGER.info(getClass(), ltx,
                    String.format("Current Edits File: %s, Last Seen TXID=%d", cf, ltx));
            updateState();
        } finally {
            __lock().unlock();
        }
    }

    private void processBatch(DFSEditLogBatch batch, HCdcTxId txId, String source) throws Exception {
        if (batch != null && batch.transactions() != null && !batch.transactions().isEmpty()) {
            long txid = txId.getId();
            for (DFSTransactionType<?> tnx : batch.transactions()) {
                if (tnx.id() <= txid) continue;
                Object proto = tnx.convertToProto(false);
                MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(proto,
                        proto.getClass(),
                        tnx.entity(source),
                        MessageObject.MessageMode.New);
                sender.send(message);
                txid = tnx.id();
            }
            txId.setId(txid);
        }
    }

    private String getPathNnCurrentDir(String path) {
        return String.format("%s/%s", path, PATH_NN_CURRENT_DIR);
    }

    @Override
    public void close() throws IOException {

    }
}
