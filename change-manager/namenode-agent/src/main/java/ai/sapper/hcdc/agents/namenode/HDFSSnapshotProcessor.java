package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.HCdcStateManager;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.filters.*;
import ai.sapper.cdc.core.messaging.*;
import ai.sapper.cdc.core.messaging.builders.MessageSenderBuilder;
import ai.sapper.cdc.core.model.*;
import ai.sapper.cdc.core.model.dfs.DFSBlockState;
import ai.sapper.cdc.core.model.dfs.DFSFileReplicaState;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.core.model.dfs.DFSReplicationOffset;
import ai.sapper.cdc.core.processing.Processor;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.core.utils.FileSystemUtils;
import ai.sapper.cdc.entity.manager.HCdcSchemaManager;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.hcdc.agents.common.*;
import ai.sapper.hcdc.agents.settings.HDFSSnapshotProcessorSettings;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.utils.ProtoUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class HDFSSnapshotProcessor extends Processor<EHCdcProcessorState, HCdcTxId> {
    private Logger LOG;
    private MessageSender<String, DFSChangeDelta> sender;
    private MessageSender<String, DFSChangeDelta> adminSender;
    private HDFSSnapshotProcessorConfig processorConfig;
    private HDFSSnapshotProcessorSettings settings;
    private HCdcSchemaManager schemaManager;

    protected HDFSSnapshotProcessor(@NonNull NameNodeEnv env) {
        super(env, HCdcProcessingState.class);
        Preconditions.checkState(stateManager() instanceof HCdcStateManager);
    }

    @Override
    public Processor<EHCdcProcessorState, HCdcTxId> init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                         String path) throws ConfigurationException {
        Preconditions.checkState(env instanceof NameNodeEnv);
        if (Strings.isNullOrEmpty(path)) {
            path = HDFSSnapshotProcessorSettings.__CONFIG_PATH;
        }
        try {
            processorConfig = new HDFSSnapshotProcessorConfig(xmlConfig, path);
            processorConfig.read();
            settings = (HDFSSnapshotProcessorSettings) processorConfig.settings();
            super.init(settings);
            sender = processorConfig.readSender(env);
            adminSender = processorConfig.readAdminSender(env);
            schemaManager = ((NameNodeEnv) env).schemaManager();
            if (schemaManager == null) {
                throw new Exception(String.format("[%s] Schema Manager not initialized...", name()));
            }
            LOG = LoggerFactory.getLogger(getClass());
            state.setState(ProcessorState.EProcessorState.Initialized);
            return this;
        } catch (Throwable ex) {
            state.error(ex);
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
    protected void doRun() throws Throwable {
        checkState();
        schemaManager.refresh();
        HCdcStateManager stateManager = (HCdcStateManager) stateManager();
        __lock().lock();
        try {
            HCdcTxId txId = stateManager.processingState().getOffset();
            int count = 0;
            for (String domain : schemaManager.matchers().keySet()) {
                DefaultLogger.info(LOG, String.format("Running snapshot for domain. [domain=%s]", domain));
                DomainFilterMatcher matcher = schemaManager.matchers().get(domain);
                if (matcher != null) {
                    List<DomainFilterMatcher.PathFilter> filters = matcher.patterns();
                    if (filters != null && !filters.isEmpty()) {
                        for (DomainFilterMatcher.PathFilter filter : filters) {
                            count += processFilter(filter, domain, txId);
                        }
                    }
                }
            }
        } finally {
            __lock().unlock();
        }
    }

    public DomainFilters addFilter(@NonNull String domain,
                                   @NonNull Filter filter,
                                   String group) throws Exception {
        checkState();
        return schemaManager.add(domain, filter.getEntity(), filter.getPath(), filter.getRegex(), group);
    }

    public DomainFilter updateGroup(@NonNull String domain,
                                    @NonNull String entity,
                                    @NonNull String group) throws Exception {
        checkState();
        return schemaManager.updateGroup(domain, entity, group);
    }

    public DomainFilter removeFilter(@NonNull String domain,
                                     @NonNull String entity) throws Exception {
        checkState();
        return schemaManager.remove(domain, entity);
    }

    public List<Filter> removeFilter(@NonNull String domain,
                                     @NonNull String entity,
                                     @NonNull String path) throws Exception {
        checkState();
        return schemaManager.remove(domain, entity, path);
    }

    public Filter removeFilter(@NonNull String domain,
                               @NonNull Filter filter) throws Exception {
        checkState();
        return schemaManager.remove(domain, filter.getEntity(), filter.getPath(), filter.getRegex());
    }

    public int processFilter(@NonNull Filter filter,
                             @NonNull String domain,
                             @NonNull HCdcTxId txId) throws Exception {
        checkState();
        DomainFilterMatcher matcher = schemaManager.matchers().get(domain);
        if (matcher == null) {
            throw new Exception(String.format("No matcher found for domain. [domain=%s]", domain));
        }
        DomainFilterMatcher.PathFilter pf = matcher.find(filter);
        if (pf == null) {
            throw new Exception(
                    String.format("Specified filter not registered. [domain=%s][filter=%s]", domain, filter.toString()));
        }
        return processFilter(pf, domain, txId);
    }

    public int processFilter(@NonNull DomainFilterMatcher.PathFilter filter,
                             String domain,
                             @NonNull HCdcTxId txId) throws Exception {
        checkState();
        FileSystem fs = schemaManager.hdfsConnection().fileSystem();
        List<Path> paths = FileSystemUtils.list(filter.path(), fs);
        int count = 0;
        if (paths != null) {
            DefaultLogger.debug(
                    String.format("Files returned for path. [path=%s][count=%d]", filter.path(), paths.size()));
            for (Path path : paths) {
                URI uri = path.toUri();
                String hdfsPath = uri.getPath();
                if (filter.matches(hdfsPath)) {
                    SchemaEntity d = new SchemaEntity();
                    d.setDomain(domain);
                    d.setEntity(filter.filter().getEntity());
                    runSnapshot(hdfsPath, d, txId);
                    count++;
                }
            }
        }
        return count;
    }

    public int processFilterWithLock(@NonNull DomainFilterMatcher.PathFilter filter,
                                     String domain,
                                     @NonNull HCdcTxId txId) throws Exception {
        checkState();
        __lock().lock();
        try {
            return processFilter(filter, domain, txId);
        } finally {
            __lock().unlock();
        }
    }

    private void runSnapshot(@NonNull String hdfsPath,
                             @NonNull SchemaEntity entity,
                             @NonNull HCdcTxId txId) throws SnapshotError {
        try {
            HCdcStateManager stateManager = (HCdcStateManager) stateManager();
            DefaultLogger.debug(LOG, String.format("Generating snapshot for file. [path=%s]", hdfsPath));
            DFSFileState fileState = stateManager
                    .fileStateHelper()
                    .get(hdfsPath);
            if (fileState == null) {
                DefaultLogger.info(LOG, String.format("HDFS File State not found. [path=%s]", hdfsPath));
                return;
            }
            if (txId.getId() < fileState.getLastTnxId()) {
                txId.setId(fileState.getLastTnxId());
            }
            DFSFileReplicaState rState = stateManager
                    .replicaStateHelper()
                    .get(entity, fileState.getFileInfo().getInodeId());
            if (rState != null) {
                if (rState.getOffset().getSnapshotTxId() > txId.getId()) {
                    return;
                }
                stateManager
                        .replicaStateHelper()
                        .delete(entity, rState.getFileInfo().getInodeId());
            }
            rState = stateManager
                    .replicaStateHelper()
                    .create(fileState.getFileInfo(), entity, true);

            long seq = ((HCdcStateManager) stateManager()).nextSequence(1);
            txId.setSequence(seq);
            txId.setType(EngineType.HDFS);

            rState.copyBlocks(fileState);

            DFSFileClose closeFile = generateSnapshot(fileState, true, txId);
            MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(closeFile,
                    DFSFileClose.class,
                    entity,
                    MessageObject.MessageMode.Snapshot);
            sender.send(message);
            DFSReplicationOffset offset = rState.getOffset();
            offset.setSnapshotTxId(fileState.getLastTnxId());
            offset.setSnapshotSeq(seq);
            rState.setSnapshotTime(System.currentTimeMillis());
            ProtoBufUtils.update(fileState, rState);
            stateManager
                    .replicaStateHelper()
                    .update(rState);
            DefaultLogger.info(LOG, String.format("Snapshot generated for path. [path=%s][inode=%d]",
                    fileState.getFileInfo().getHdfsPath(), fileState.getFileInfo().getInodeId()));
        } catch (Exception ex) {
            throw new SnapshotError(ex);
        }
    }

    public DFSFileReplicaState snapshotDone(@NonNull String hdfsPath,
                                            @NonNull SchemaEntity entity,
                                            @NonNull HCdcTxId txId) throws SnapshotError {
        try {
            checkState();
            __lock().lock();
            try {
                return processDone(hdfsPath, entity, txId);
            } finally {
                __lock().unlock();
            }
        } catch (SnapshotError se) {
            throw se;
        } catch (Exception ex) {
            throw new SnapshotError(ex);
        }
    }

    private DFSFileReplicaState processDone(@NonNull String hdfsPath,
                                            @NonNull SchemaEntity entity,
                                            @NonNull HCdcTxId tnxId) throws Exception {
        HCdcStateManager stateManager = (HCdcStateManager) stateManager();
        DFSFileState fileState = stateManager
                .fileStateHelper()
                .get(hdfsPath);
        if (fileState == null) {
            throw new SnapshotError(String.format("HDFS File State not found. [path=%s]", hdfsPath));
        }
        DFSFileReplicaState rState = stateManager
                .replicaStateHelper()
                .get(entity,
                        fileState.getFileInfo().getInodeId());
        if (rState == null) {
            throw new SnapshotError(String.format("HDFS File replication record not found. [path=%s]", hdfsPath));
        }
        DFSReplicationOffset offset = rState.getOffset();
        if (tnxId.getId() != offset.getSnapshotTxId()) {
            throw new SnapshotError(
                    String.format("Snapshot transaction mismatch. [expected=%d][actual=%s]",
                            offset.getSnapshotTxId(), tnxId));
        }
        if (rState.isSnapshotReady()) {
            DefaultLogger.warn(LOG, String.format("Duplicate Call: Snapshot Done: [path=%s]", rState.getFileInfo().getHdfsPath()));
            return rState;
        }
        long lastTxId = tnxId.getId();
        if (fileState.getLastTnxId() > offset.getSnapshotTxId()) {
            DFSFileClose closeFile = generateSnapshot(fileState, true, tnxId);
            MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(closeFile,
                    DFSFileClose.class,
                    entity,
                    MessageObject.MessageMode.Backlog);
            adminSender.send(message);
            lastTxId = fileState.getLastTnxId();
        }
        rState.setSnapshotReady(true);
        rState.setSnapshotTime(System.currentTimeMillis());
        rState.setLastReplicationTime(System.currentTimeMillis());
        offset.setLastReplicatedTxId(lastTxId);
        rState.setFileState(EFileState.Finalized);
        ProtoBufUtils.update(fileState, rState);
        stateManager
                .replicaStateHelper()
                .update(rState);
        return rState;
    }

    public static DFSFileClose generateSnapshot(@NonNull DFSFileState state,
                                                @NonNull DFSFile file,
                                                @NonNull HCdcTxId txId) throws Exception {
        DFSTransaction tx = ProtoUtils.buildTx(txId, DFSTransaction.Operation.CLOSE);

        DFSFileClose.Builder builder = DFSFileClose.newBuilder();
        builder.setOverwrite(false)
                .setModifiedTime(state.getUpdatedTime())
                .setBlockSize(state.getBlockSize())
                .setFile(file)
                .setTransaction(tx)
                .setLength(state.getDataSize())
                .setAccessedTime(state.getUpdatedTime());
        for (DFSBlockState block : state.sortedBlocks()) {
            DFSBlock b = generateBlockSnapshot(block);
            builder.addBlocks(b);
        }
        return builder.build();
    }

    public static DFSFileClose generateSnapshot(@NonNull DFSFileState state,
                                                boolean addBlocks,
                                                @NonNull HCdcTxId txId) throws Exception {
        DFSTransaction tx = ProtoUtils.buildTx(txId, DFSTransaction.Operation.CLOSE);

        DFSFile file = state.getFileInfo().proto();
        DFSFileClose.Builder builder = DFSFileClose.newBuilder();
        builder.setOverwrite(false)
                .setModifiedTime(state.getUpdatedTime())
                .setBlockSize(state.getBlockSize())
                .setFile(file)
                .setTransaction(tx)
                .setLength(state.getDataSize())
                .setAccessedTime(state.getUpdatedTime());
        if (addBlocks) {
            for (DFSBlockState block : state.sortedBlocks()) {
                DFSBlock b = generateBlockSnapshot(block);
                builder.addBlocks(b);
            }
        }
        return builder.build();
    }

    public static DFSBlock generateBlockSnapshot(DFSBlockState block) throws Exception {
        DFSBlock.Builder builder = DFSBlock.newBuilder();
        long eoff = (block.getDataSize() > 0 ? block.getDataSize() - 1 : 0);
        builder.setBlockId(block.getBlockId())
                .setGenerationStamp(block.getGenerationStamp())
                .setSize(block.getDataSize())
                .setBlockSize(block.getBlockSize())
                .setStartOffset(0)
                .setEndOffset(eoff)
                .setDeltaSize(block.getDataSize());
        return builder.build();
    }

    private void checkState() throws Exception {
        if (state.isAvailable() || state.isInitialized()) {
            return;
        }
        throw new Exception(String.format("[%s] Processor not available.", name()));
    }

    @Override
    public void close() throws IOException {
        if (sender != null) {
            sender.close();
            sender = null;
        }
        if (adminSender != null) {
            adminSender.close();
            adminSender = null;
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class HDFSSnapshotProcessorConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "processor.snapshot";
        private static final String CONFIG_EXECUTOR_POOL_SIZE = "executorPoolSize";

        public HDFSSnapshotProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH, HDFSSnapshotProcessorSettings.class);
        }

        public HDFSSnapshotProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, path, HDFSSnapshotProcessorSettings.class);
        }

        @SuppressWarnings("unchecked")
        public MessageSender<String, DFSChangeDelta> readSender(@NonNull BaseEnv<?> env) throws Exception {
            HDFSSnapshotProcessorSettings settings = (HDFSSnapshotProcessorSettings) settings();
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

        @SuppressWarnings("unchecked")
        public MessageSender<String, DFSChangeDelta> readAdminSender(@NonNull BaseEnv<?> env) throws Exception {
            HDFSSnapshotProcessorSettings settings = (HDFSSnapshotProcessorSettings) settings();
            MessageSenderBuilder<String, DFSChangeDelta> builder
                    = (MessageSenderBuilder<String, DFSChangeDelta>) settings.getAdminBuilderType()
                    .getDeclaredConstructor(BaseEnv.class, Class.class)
                    .newInstance(env, settings.getAdminBuilderSettingsType());
            HierarchicalConfiguration<ImmutableNode> eConfig
                    = config().configurationAt(HDFSSnapshotProcessorSettings.__CONFIG_PATH_ADMIN);
            if (eConfig == null) {
                throw new ConfigurationException(
                        String.format("Admin Sender queue configuration not found. [path=%s]",
                                HDFSSnapshotProcessorSettings.__CONFIG_PATH_ADMIN));
            }
            return builder.build(eConfig);
        }
    }

    public static class SnapshotCallBack implements FilterAddCallback {
        private final HDFSSnapshotProcessor processor;
        private final ThreadPoolExecutor executor;

        public SnapshotCallBack(@NonNull HDFSSnapshotProcessor processor, int corePoolSize) {
            Preconditions.checkArgument(corePoolSize > 0);
            this.processor = processor;
            executor = new ThreadPoolExecutor(corePoolSize,
                    corePoolSize,
                    30000,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());
        }


        /**
         * @param matcher
         * @param filter
         * @param path
         */
        @Override
        public void process(@NonNull DomainFilterMatcher matcher,
                            DomainFilterMatcher.PathFilter filter,
                            @NonNull String path) {
            SnapshotRunner runner = new SnapshotRunner(processor, matcher, filter);
            executor.submit(runner);
        }

        /**
         * @param matcher
         */
        @Override
        public void onStart(@NonNull DomainFilterMatcher matcher) {

        }
    }

    public static class SnapshotRunner implements Runnable {
        private final HDFSSnapshotProcessor processor;
        private final DomainFilterMatcher matcher;
        private final DomainFilterMatcher.PathFilter filter;

        public SnapshotRunner(@NonNull HDFSSnapshotProcessor processor,
                              @NonNull DomainFilterMatcher matcher,
                              @NonNull DomainFilterMatcher.PathFilter filter) {
            this.processor = processor;
            this.matcher = matcher;
            this.filter = filter;
        }

        @Override
        public void run() {
            try {
                DefaultLogger.info(processor.LOG,
                        String.format("Processing filer: [filter=%s]",
                                JSONUtils.asString(filter.filter(), Filter.class)));
                int count = processor.processFilterWithLock(filter, matcher.domain(), new HCdcTxId(-1));
                DefaultLogger.info(processor.LOG,
                        String.format("Processed filer: [filter=%s][files=%d]",
                                JSONUtils.asString(filter.filter(), Filter.class), count));
            } catch (Exception ex) {
                DefaultLogger.error(processor.LOG,
                        String.format("Error processing filter: %s", filter.filter().toString()), ex);
                DefaultLogger.stacktrace(ex);
            }
        }
    }
}
