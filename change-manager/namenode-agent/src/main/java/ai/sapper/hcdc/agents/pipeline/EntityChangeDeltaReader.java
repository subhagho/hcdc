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

import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.WebServiceClient;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.EncryptionHandler;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.messaging.ReceiverOffset;
import ai.sapper.cdc.core.model.EHCdcProcessorState;
import ai.sapper.cdc.core.model.HCdcMessageProcessingState;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.model.Params;
import ai.sapper.cdc.core.processing.MessageProcessorState;
import ai.sapper.cdc.core.processing.ProcessingState;
import ai.sapper.cdc.core.utils.ProtoUtils;
import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.settings.EntityChangeDeltaReaderSettings;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

@Getter
@Accessors(fluent = true)
public class EntityChangeDeltaReader<MO extends ReceiverOffset> extends ChangeDeltaProcessor<MO> {
    private static Logger LOG = LoggerFactory.getLogger(EntityChangeDeltaReader.class);

    private FileSystem fs;
    private Archiver archiver;

    private HdfsConnection connection;
    private FileSystem.FileSystemMocker fileSystemMocker;
    private WebServiceClient client;
    private EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler;

    public EntityChangeDeltaReader(@NonNull NameNodeEnv env) {
        super(env, EntityChangeDeltaReaderSettings.class, EProcessorMode.Committer, true);
    }

    public EntityChangeDeltaReader<MO> withMockFileSystem(@NonNull FileSystem.FileSystemMocker fileSystemMocker) {
        this.fileSystemMocker = fileSystemMocker;
        return this;
    }


    /**
     * @param xmlConfig
     * @return
     * @throws ConfigurationException
     */
    @Override
    public ChangeDeltaProcessor<MO> init(@NonNull String name,
                                         @NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            super.init(name, xmlConfig, null);
            ConnectionManager manger = env().connectionManager();
            EntityChangeTransactionReader processor = new EntityChangeTransactionReader(name(), env());
            EntityChangeDeltaReaderSettings settings = (EntityChangeDeltaReaderSettings) receiverConfig.settings();
            connection = manger.getConnection(settings.getHdfsConnection(), HdfsConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("HDFS Connection not found. [name=%s]", settings.getHdfsConnection()));
            }
            if (!connection.isConnected()) connection.connect();

            if (fileSystemMocker == null) {
                fs = env().fileSystemManager().read(receiverConfig.config());
            } else {
                fs = fileSystemMocker.create(receiverConfig.config(), env());
            }
            client = new WebServiceClient();
            client.init(receiverConfig.config(),
                    EntityChangeDeltaReaderSettings.Constants.CONFIG_WS_PATH,
                    manger);

            /*
             * TODO: Add archival
             *
            if (!Strings.isNullOrEmpty(config.archiverClass)) {
                Class<? extends Archiver> cls = (Class<? extends Archiver>) Class.forName(config.archiverClass);
                archiver = cls.getDeclaredConstructor().newInstance();
                archiver.init(config.config(), Archiver.CONFIG_ARCHIVER);
            }
            */

            processor.withFileSystem(fs)
                    .withArchiver(archiver)
                    .withHdfsConnection(connection)
                    .withClient(client)
                    .withSenderQueue(sender())
                    .withErrorQueue(errorLogger);

            if (settings.getEncryptorClass() != null) {
                encryptionHandler = settings.getEncryptorClass().getDeclaredConstructor().newInstance();
                encryptionHandler.init(receiverConfig.config());
                processor.withEncryptionHandler(encryptionHandler);
            }
            return withProcessor(processor);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    protected void initState(@NonNull ProcessingState<EHCdcProcessorState, HCdcTxId> processingState) throws Exception {

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
                        @NonNull Params params) throws Exception {
        HCdcTxId txId = null;
        if (params.dfsTx() != null) {
            txId = ProtoUtils.fromTx(params.dfsTx());
        } else {
            txId = new HCdcTxId(-1);
        }
        params.txId(txId);
        EntityChangeTransactionReader processor
                = (EntityChangeTransactionReader) processor();
        processor.processTxMessage(message, data, params);
    }

    @Override
    protected void batchStart(@NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> messageProcessorState) throws Exception {

    }

    @Override
    protected void batchEnd(@NonNull MessageProcessorState<EHCdcProcessorState, HCdcTxId, MO> messageProcessorState) throws Exception {

    }
}
