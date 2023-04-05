package ai.sapper.hcdc.agents.pipeline;

import ai.sapper.cdc.core.WebServiceClient;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.EncryptionHandler;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.cdc.core.model.BaseTxId;
import ai.sapper.hcdc.agents.common.ChangeDeltaProcessor;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import ai.sapper.hcdc.common.model.DFSTransaction;
import ai.sapper.cdc.core.utils.ProtoUtils;
import com.google.common.base.Strings;
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
public class EntityChangeDeltaReader extends ChangeDeltaProcessor {
    private static Logger LOG = LoggerFactory.getLogger(EntityChangeDeltaReader.class);

    private CDCFileSystem fs;
    private Archiver archiver;

    private EntityChangeDeltaReaderConfig config;
    private HdfsConnection connection;
    private CDCFileSystem.FileSystemMocker fileSystemMocker;
    private WebServiceClient client;
    private EncryptionHandler<ByteBuffer, ByteBuffer> encryptionHandler;

    public EntityChangeDeltaReader(@NonNull ZkStateManager stateManager, @NonNull String name) {
        super(stateManager, name, EProcessorMode.Committer, true);
    }

    public EntityChangeDeltaReader withMockFileSystem(@NonNull CDCFileSystem.FileSystemMocker fileSystemMocker) {
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
            config = new EntityChangeDeltaReaderConfig(xmlConfig);
            config.read();

            super.init(config, manger);

            EntityChangeTransactionReader processor = new EntityChangeTransactionReader(name());

            connection = manger.getConnection(config.hdfsConnection, HdfsConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("HDFS Connection not found. [name=%s]", config.hdfsConnection));
            }
            if (!connection.isConnected()) connection.connect();

            if (fileSystemMocker == null) {
                Class<? extends CDCFileSystem> fsc = (Class<? extends CDCFileSystem>) Class.forName(config.fsType);
                fs = fsc.getDeclaredConstructor().newInstance();
                fs.init(config.config(),
                        EntityChangeDeltaReaderConfig.Constants.CONFIG_PATH_FS,
                        env().keyStore());
            } else {
                fs = (CDCFileSystem) fileSystemMocker.create(config.config());
            }
            client = new WebServiceClient();
            client.init(config.config(),
                    EntityChangeDeltaReaderConfig.Constants.CONFIG_WS_PATH,
                    manger);

            if (!Strings.isNullOrEmpty(config.archiverClass)) {
                Class<? extends Archiver> cls = (Class<? extends Archiver>) Class.forName(config.archiverClass);
                archiver = cls.getDeclaredConstructor().newInstance();
                archiver.init(config.config(), Archiver.CONFIG_ARCHIVER);
            }

            processor.withFileSystem(fs)
                    .withArchiver(archiver)
                    .withHdfsConnection(connection)
                    .withClient(client)
                    .withSenderQueue(sender())
                    .withStateManager(stateManager())
                    .withErrorQueue(errorSender());

            if (config.encryptorClass != null) {
                encryptionHandler = config.encryptorClass.getDeclaredConstructor().newInstance();
                encryptionHandler.init(config.config());
                processor.withEncryptionHandler(encryptionHandler);
            }
            return withProcessor(processor);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
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
    public void batchStart() throws Exception {

    }

    @Override
    public void batchEnd() throws Exception {

    }

    @Override
    public void process(@NonNull MessageObject<String, DFSChangeDelta> message,
                        @NonNull Object data,
                        DFSTransaction tnx,
                        boolean retry) throws Exception {
        BaseTxId txId = null;
        if (tnx != null) {
            txId = ProtoUtils.fromTx(tnx);
        } else {
            txId = new BaseTxId(-1);
        }
        EntityChangeTransactionReader processor
                = (EntityChangeTransactionReader) processor();
        processor.processTxMessage(message, data, txId, retry);
    }

    @Getter
    @Accessors(fluent = true)
    public static class EntityChangeDeltaReaderConfig extends ChangeDeltaProcessorConfig {
        public static final String __CONFIG_PATH = "processor.files";

        public static class Constants {
            public static final String CONFIG_PATH_FS = "filesystem";
            public static final String CONFIG_FS_TYPE = String.format("%s.type", CONFIG_PATH_FS);
            public static final String CONFIG_HDFS_CONN = "hdfs";
            public static final String CONFIG_WS_PATH = "snapshot";
            public static final String CONFIG_ARCHIVER_CLASS = String.format("%s.class", Archiver.CONFIG_ARCHIVER);
            public static final String CONFIG_ENCRYPTOR_CLASS = String.format("%s.class", EncryptionHandler.__CONFIG_PATH);
        }

        private String fsType;
        private String hdfsConnection;
        private String archiverClass;
        private HierarchicalConfiguration<ImmutableNode> fsConfig;
        private Class<? extends EncryptionHandler<ByteBuffer, ByteBuffer>> encryptorClass;

        public EntityChangeDeltaReaderConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        /**
         * @throws ConfigurationException
         */
        @SuppressWarnings("unchecked")
        @Override
        public void read() throws ConfigurationException {
            super.read();

            try {
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
                if (get().containsKey(Constants.CONFIG_ARCHIVER_CLASS)) {
                    archiverClass = get().getString(Constants.CONFIG_ARCHIVER_CLASS);
                }
                if (checkIfNodeExists(get(), EncryptionHandler.__CONFIG_PATH)) {
                    String s = get().getString(Constants.CONFIG_ENCRYPTOR_CLASS);
                    checkStringValue(s, getClass(), Constants.CONFIG_ENCRYPTOR_CLASS);
                    encryptorClass = (Class<? extends EncryptionHandler<ByteBuffer, ByteBuffer>>) Class.forName(s);
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }
}
