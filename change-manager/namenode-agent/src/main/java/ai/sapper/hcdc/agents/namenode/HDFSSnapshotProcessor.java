package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.filters.DomainFilter;
import ai.sapper.cdc.common.filters.DomainFilterMatcher;
import ai.sapper.cdc.common.filters.DomainFilters;
import ai.sapper.cdc.common.filters.Filter;
import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.filters.DomainManager;
import ai.sapper.cdc.core.filters.FilterAddCallback;
import ai.sapper.cdc.core.messaging.*;
import ai.sapper.cdc.core.utils.FileSystemUtils;
import ai.sapper.hcdc.agents.common.*;
import ai.sapper.hcdc.agents.model.DFSBlockState;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileState;
import ai.sapper.hcdc.agents.model.EFileState;
import ai.sapper.hcdc.common.model.*;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.slf4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public class HDFSSnapshotProcessor {
    private Logger LOG;
    private final String name;
    private final ZkStateManager stateManager;
    private MessageSender<String, DFSChangeDelta> sender;
    private MessageSender<String, DFSChangeDelta> tnxSender;
    private HDFSSnapshotProcessorConfig processorConfig;

    public HDFSSnapshotProcessor(@NonNull ZkStateManager stateManager,
                                 @NonNull String name) {
        this.stateManager = stateManager;
        this.name = name;
    }

    public HDFSSnapshotProcessor init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                      @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            processorConfig = new HDFSSnapshotProcessorConfig(xmlConfig);
            processorConfig.read();

            sender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.senderConfig.config())
                    .manager(manger)
                    .connection(processorConfig().senderConfig.connection())
                    .type(processorConfig().senderConfig.type())
                    .partitioner(processorConfig().senderConfig.partitionerClass())
                    .auditLogger(NameNodeEnv.get(name).auditLogger())
                    .build();

            tnxSender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.tnxSenderConfig.config())
                    .manager(manger)
                    .connection(processorConfig().tnxSenderConfig.connection())
                    .type(processorConfig().tnxSenderConfig.type())
                    .partitioner(processorConfig().tnxSenderConfig.partitionerClass())
                    .auditLogger(NameNodeEnv.get(name).auditLogger())
                    .build();
            if (stateManager instanceof ProcessorStateManager) {
                ((ProcessorStateManager) stateManager)
                        .domainManager()
                        .withFilterAddCallback(new SnapshotCallBack(this, processorConfig().executorPoolSize));
            }
            LOG = NameNodeEnv.get(name).LOG;
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public int run() throws Exception {
        Preconditions.checkState(stateManager instanceof ProcessorStateManager);
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        Preconditions.checkNotNull(domainManager.hdfsConnection());
        domainManager.refresh();

        int count = 0;
        long txId = stateManager.getModuleState().getReceivedTxId();
        try (DistributedLock lock = NameNodeEnv.get(name).globalLock()
                .withLockTimeout(processorConfig.defaultLockTimeout)) {
            if (DistributedLock.withRetry(lock, 5, 1000)) {
                try {
                    for (String domain : domainManager.matchers().keySet()) {
                        DefaultLogger.LOGGER.info(String.format("Running snapshot for domain. [domain=%s]", domain));
                        DomainFilterMatcher matcher = domainManager.matchers().get(domain);
                        if (matcher != null) {
                            List<DomainFilterMatcher.PathFilter> filters = matcher.patterns();
                            if (filters != null && !filters.isEmpty()) {
                                for (DomainFilterMatcher.PathFilter filter : filters) {
                                    count += processFilter(filter, domain, txId);
                                }
                            }
                        }
                    }
                    stateManager.updateSnapshotTx(txId);
                    return count;
                } finally {
                    lock.unlock();
                }
            } else {
                throw new Exception(String.format("Failed to acquire lock. [lock=%s]", lock.toString()));
            }
        }
    }

    public DomainFilters addFilter(@NonNull String domain,
                                   @NonNull Filter filter,
                                   String group) throws Exception {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        return domainManager.add(domain, filter.getEntity(), filter.getPath(), filter.getRegex(), group);
    }

    public DomainFilter updateGroup(@NonNull String domain,
                                    @NonNull String entity,
                                    @NonNull String group) {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        return domainManager.updateGroup(domain, entity, group);
    }

    public DomainFilter removeFilter(@NonNull String domain,
                                     @NonNull String entity) throws Exception {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        return domainManager.remove(domain, entity);
    }

    public List<Filter> removeFilter(@NonNull String domain,
                                     @NonNull String entity,
                                     @NonNull String path) throws Exception {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        return domainManager.remove(domain, entity, path);
    }

    public Filter removeFilter(@NonNull String domain,
                               @NonNull Filter filter) throws Exception {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        return domainManager.remove(domain, filter.getEntity(), filter.getPath(), filter.getRegex());
    }

    public int processFilter(@NonNull Filter filter,
                             @NonNull String domain,
                             long txId) throws Exception {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        DomainFilterMatcher matcher = domainManager.matchers().get(domain);
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
                             long txId) throws Exception {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        FileSystem fs = domainManager.hdfsConnection().fileSystem();
        List<Path> paths = FileSystemUtils.list(filter.path(), fs);
        int count = 0;
        if (paths != null) {
            DefaultLogger.LOGGER.debug(
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
                                     long txId) throws Exception {
        try (DistributedLock lock = NameNodeEnv.get(name).globalLock()
                .withLockTimeout(processorConfig().defaultLockTimeout())) {
            if (DistributedLock.withRetry(lock, 5, 500)) {
                try {
                    return processFilter(filter, domain, txId);
                } finally {
                    lock.unlock();
                }
            } else {
                throw new Exception(String.format("Failed to acquire lock. [lock=%s]", lock.toString()));
            }
        }
    }

    private void runSnapshot(@NonNull String hdfsPath,
                             @NonNull SchemaEntity entity,
                             long txId) throws SnapshotError {
        try {
            DefaultLogger.debug(LOG, String.format("Generating snapshot for file. [path=%s]", hdfsPath));
            DFSFileState fileState = stateManager
                    .fileStateHelper()
                    .get(hdfsPath);
            if (fileState == null) {
                DefaultLogger.info(LOG, String.format("HDFS File State not found. [path=%s]", hdfsPath));
                return;
            }
            if (txId < fileState.getLastTnxId()) {
                txId = fileState.getLastTnxId();
            }
            DFSFileReplicaState rState = stateManager
                    .replicaStateHelper()
                    .get(entity, fileState.getFileInfo().getInodeId());
            if (rState != null) {
                if (rState.getSnapshotTxId() > txId) {
                    return;
                }
                stateManager
                        .replicaStateHelper()
                        .delete(entity, rState.getFileInfo().getInodeId());
            }
            rState = stateManager
                    .replicaStateHelper()
                    .create(fileState.getFileInfo(), entity, true);


            rState.copyBlocks(fileState);

            DFSFileClose closeFile = generateSnapshot(fileState, true, fileState.getLastTnxId());
            long seq = stateManager().nextSnapshotSeq();
            MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(closeFile,
                    DFSFileClose.class,
                    entity,
                    seq,
                    MessageObject.MessageMode.Snapshot);
            sender.send(message);

            rState.setSnapshotTxId(fileState.getLastTnxId());
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
                                            long txId) throws SnapshotError {
        Preconditions.checkState(tnxSender != null);
        try {
            try (DistributedLock lock = NameNodeEnv.get(name).globalLock()
                    .withLockTimeout(processorConfig().defaultLockTimeout())) {
                if (DistributedLock.withRetry(lock, 5, 500)) {
                    try {
                        return processDone(hdfsPath, entity, txId);
                    } finally {
                        lock.unlock();
                    }
                } else {
                    throw new SnapshotError(String.format("Failed to acquire lock. [lock=%s]", lock.toString()));
                }
            }
        } catch (SnapshotError se) {
            throw se;
        } catch (Exception ex) {
            throw new SnapshotError(ex);
        }
    }

    private DFSFileReplicaState processDone(@NonNull String hdfsPath,
                                            @NonNull SchemaEntity entity,
                                            long tnxId) throws Exception {
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
        if (tnxId != rState.getSnapshotTxId()) {
            throw new SnapshotError(String.format("Snapshot transaction mismatch. [expected=%d][actual=%d]", rState.getSnapshotTxId(), tnxId));
        }
        if (rState.isSnapshotReady()) {
            DefaultLogger.warn(LOG, String.format("Duplicate Call: Snapshot Done: [path=%s]", rState.getFileInfo().getHdfsPath()));
            return rState;
        }
        long lastTxId = tnxId;
        if (fileState.getLastTnxId() > rState.getSnapshotTxId()) {
            DFSFileClose closeFile = generateSnapshot(fileState, true, tnxId);
            MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(closeFile,
                    DFSFileClose.class,
                    entity,
                    -1,
                    MessageObject.MessageMode.Backlog);
            tnxSender.send(message);
            lastTxId = fileState.getLastTnxId();
        }
        rState.setSnapshotReady(true);
        rState.setSnapshotTime(System.currentTimeMillis());
        rState.setLastReplicationTime(System.currentTimeMillis());
        rState.setLastReplicatedTx(lastTxId);
        rState.setState(EFileState.Finalized);
        ProtoBufUtils.update(fileState, rState);
        stateManager
                .replicaStateHelper()
                .update(rState);
        return rState;
    }

    public static DFSFileClose generateSnapshot(@NonNull DFSFileState state,
                                                @NonNull DFSFile file,
                                                long txId) throws Exception {
        DFSTransaction tx = DFSTransaction.newBuilder()
                .setOp(DFSTransaction.Operation.CLOSE)
                .setTransactionId(txId)
                .setTimestamp(System.currentTimeMillis())
                .build();

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
                                                long txId) throws Exception {
        DFSTransaction tx = DFSTransaction.newBuilder()
                .setOp(DFSTransaction.Operation.CLOSE)
                .setTransactionId(txId)
                .setTimestamp(System.currentTimeMillis())
                .build();

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

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class HDFSSnapshotProcessorConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "processor.snapshot";
        private static final String __CONFIG_PATH_SENDER = "sender";
        private static final String __CONFIG_PATH_TNX_SENDER = "tnxSender";
        private static final String CONFIG_LOCK_TIMEOUT = "lockTimeout";
        private static final String CONFIG_EXECUTOR_POOL_SIZE = "executorPoolSize";

        private MessagingConfig senderConfig;
        private MessagingConfig tnxSenderConfig;
        private long defaultLockTimeout = 5 * 60 * 1000;
        private int executorPoolSize = 4;

        public HDFSSnapshotProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public HDFSSnapshotProcessorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, path);
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("Kafka Configuration not drt or is NULL");
            }
            try {
                HierarchicalConfiguration<ImmutableNode> config = get().configurationAt(__CONFIG_PATH_SENDER);
                if (config == null) {
                    throw new ConfigurationException(String.format("Sender configuration node not found. [path=%s]", __CONFIG_PATH_SENDER));
                }
                senderConfig = new MessagingConfig();
                senderConfig.read(config);

                config = get().configurationAt(__CONFIG_PATH_TNX_SENDER);
                if (config == null) {
                    throw new ConfigurationException(String.format("Sender configuration node not found. [path=%s]", __CONFIG_PATH_TNX_SENDER));
                }
                tnxSenderConfig = new MessagingConfig();
                tnxSenderConfig.read(config);

                if (get().containsKey(CONFIG_LOCK_TIMEOUT)) {
                    String s = get().getString(CONFIG_LOCK_TIMEOUT);
                    if (!Strings.isNullOrEmpty(s)) {
                        defaultLockTimeout = Long.parseLong(s);
                    }
                }
                if (get().containsKey(CONFIG_EXECUTOR_POOL_SIZE)) {
                    String s = get().getString(CONFIG_EXECUTOR_POOL_SIZE);
                    executorPoolSize = Integer.parseInt(s);
                }
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
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
                int count = processor.processFilterWithLock(filter, matcher.domain(), -1);
                DefaultLogger.info(processor.LOG,
                        String.format("Processed filer: [filter=%s][files=%d]",
                                JSONUtils.asString(filter.filter(), Filter.class), count));
            } catch (Exception ex) {
                DefaultLogger.error(processor.LOG,
                        String.format("Error processing filter: %s", filter.filter().toString()), ex);
                DefaultLogger.debug(processor.LOG, DefaultLogger.stacktrace(ex));
            }
        }
    }
}
