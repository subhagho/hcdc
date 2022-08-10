package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.common.*;
import ai.sapper.hcdc.agents.model.DFSBlockReplicaState;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.hcdc.common.ConfigReader;
import ai.sapper.hcdc.common.filters.DomainFilter;
import ai.sapper.hcdc.common.filters.DomainFilterMatcher;
import ai.sapper.hcdc.common.filters.DomainFilters;
import ai.sapper.hcdc.common.filters.Filter;
import ai.sapper.hcdc.common.model.*;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.common.utils.JSONUtils;
import ai.sapper.hcdc.core.DistributedLock;
import ai.sapper.hcdc.core.connections.ConnectionManager;
import ai.sapper.hcdc.core.filters.DomainManager;
import ai.sapper.hcdc.core.filters.FilterAddCallback;
import ai.sapper.hcdc.core.messaging.*;
import ai.sapper.hcdc.core.model.DFSBlockState;
import ai.sapper.hcdc.core.model.DFSFileState;
import ai.sapper.hcdc.core.model.EFileState;
import ai.sapper.hcdc.core.model.EFileType;
import ai.sapper.hcdc.core.utils.FileSystemUtils;
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

import java.net.URI;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class HDFSSnapshotProcessor {
    private final ZkStateManager stateManager;
    private MessageSender<String, DFSChangeDelta> sender;
    private MessageSender<String, DFSChangeDelta> tnxSender;
    private HDFSSnapshotProcessorConfig processorConfig;

    public HDFSSnapshotProcessor(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
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
                    .build();

            tnxSender = new HCDCMessagingBuilders.SenderBuilder()
                    .config(processorConfig.tnxSenderConfig.config())
                    .manager(manger)
                    .connection(processorConfig().tnxSenderConfig.connection())
                    .type(processorConfig().tnxSenderConfig.type())
                    .partitioner(processorConfig().tnxSenderConfig.partitionerClass())
                    .build();
            if (stateManager instanceof ProcessorStateManager) {
                ((ProcessorStateManager) stateManager)
                        .domainManager()
                        .withFilterAddCallback(new SnapshotCallBack(this));
            }
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public int run() throws Exception {
        Preconditions.checkState(stateManager instanceof ProcessorStateManager);
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        Preconditions.checkNotNull(domainManager.hdfsConnection());
        int count = 0;
        try (DistributedLock lock = NameNodeEnv.globalLock()
                .withLockTimeout(processorConfig.defaultLockTimeout)) {
            lock.lock();
            try {
                for (String domain : domainManager.matchers().keySet()) {
                    DomainFilterMatcher matcher = domainManager.matchers().get(domain);
                    if (matcher != null) {
                        List<DomainFilterMatcher.PathFilter> filters = matcher.patterns();
                        if (filters != null && !filters.isEmpty()) {
                            for (DomainFilterMatcher.PathFilter filter : filters) {
                                count += processFilter(filter, domain);
                            }
                        }
                    }
                }
                return count;
            } finally {
                lock.unlock();
            }
        }
    }

    public DomainFilters addFilter(@NonNull String domain,
                                   @NonNull Filter filter) throws Exception {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        return domainManager.add(domain, filter.getEntity(), filter.getPath(), filter.getRegex());
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
                             @NonNull String domain) throws Exception {
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
        return processFilter(pf, domain);
    }

    public int processFilter(@NonNull DomainFilterMatcher.PathFilter filter,
                             String domain) throws Exception {
        DomainManager domainManager = ((ProcessorStateManager) stateManager).domainManager();
        FileSystem fs = domainManager.hdfsConnection().fileSystem();
        List<Path> paths = FileSystemUtils.list(filter.path(), fs);
        int count = 0;
        if (paths != null) {
            for (Path path : paths) {
                URI uri = path.toUri();
                String hdfsPath = uri.getPath();
                if (filter.matches(hdfsPath)) {
                    SchemaEntity d = new SchemaEntity();
                    d.setDomain(domain);
                    d.setEntity(filter.filter().getEntity());
                    snapshot(hdfsPath, d);
                    count++;
                }
            }
        }
        return count;
    }

    public int processFilterWithLock(@NonNull DomainFilterMatcher.PathFilter filter,
                                     String domain) throws Exception {
        try (DistributedLock lock = NameNodeEnv.globalLock()
                .withLockTimeout(processorConfig().defaultLockTimeout())) {
            lock.lock();
            try {
                return processFilter(filter, domain);
            } finally {
                lock.unlock();
            }
        }
    }

    public void snapshot(@NonNull String hdfsPath, @NonNull SchemaEntity entity) throws SnapshotError {
        Preconditions.checkState(sender != null);
        stateManager.replicationLock().lock();
        try {
            DefaultLogger.LOG.debug(String.format("Generating snapshot for file. [path=%s]", hdfsPath));
            DFSFileState fileState = stateManager
                    .fileStateHelper()
                    .get(hdfsPath);
            if (fileState == null) {
                DefaultLogger.LOG.info(String.format("HDFS File State not found. [path=%s]", hdfsPath));
                return;
            }
            DFSFileReplicaState rState = stateManager
                    .replicaStateHelper()
                    .get(fileState.getId());
            if (rState == null) {
                rState = stateManager
                        .replicaStateHelper()
                        .create(fileState.getId(), fileState.getHdfsFilePath(), entity, true);
            }
            if (rState.getSnapshotTxId() > 0) {
                return;
            }
            if (fileState.getFileType() != null
                    && fileState.getFileType() != EFileType.UNKNOWN) {
                rState.setFileType(fileState.getFileType());
                rState.setSchemaLocation(fileState.getSchemaLocation());
            }
            rState.copyBlocks(fileState);

            DFSCloseFile closeFile = generateSnapshot(fileState, true, fileState.getLastTnxId());
            MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(NameNodeEnv.get().source(),
                    closeFile,
                    DFSCloseFile.class,
                    entity.getDomain(),
                    entity.getEntity(),
                    MessageObject.MessageMode.Snapshot);
            sender.send(message);

            rState.setSnapshotTxId(fileState.getLastTnxId());
            rState.setSnapshotTime(System.currentTimeMillis());
            ProtoBufUtils.update(fileState, rState);
            stateManager
                    .replicaStateHelper()
                    .update(rState);
            DefaultLogger.LOG.info(String.format("Snapshot generated for path. [path=%s][inode=%d]", fileState.getHdfsFilePath(), fileState.getId()));
        } catch (SnapshotError se) {
            throw se;
        } catch (Exception ex) {
            throw new SnapshotError(ex);
        } finally {
            stateManager.replicationLock().unlock();
        }
    }

    public DFSFileReplicaState snapshotDone(@NonNull String hdfsPath, @NonNull SchemaEntity entity, long tnxId) throws SnapshotError {
        Preconditions.checkState(tnxSender != null);
        stateManager.replicationLock().lock();
        try {
            DFSFileState fileState = stateManager
                    .fileStateHelper()
                    .get(hdfsPath);
            if (fileState == null) {
                throw new SnapshotError(String.format("HDFS File State not found. [path=%s]", hdfsPath));
            }
            DFSFileReplicaState rState = stateManager
                    .replicaStateHelper()
                    .get(fileState.getId());
            if (rState == null) {
                throw new SnapshotError(String.format("HDFS File replication record not found. [path=%s]", hdfsPath));
            }
            if (tnxId != rState.getSnapshotTxId()) {
                throw new SnapshotError(String.format("Snapshot transaction mismatch. [expected=%d][actual=%d]", rState.getSnapshotTxId(), tnxId));
            }
            if (rState.isSnapshotReady()) {
                DefaultLogger.LOG.warn(String.format("Duplicate Call: Snapshot Done: [path=%s]", rState.getHdfsPath()));
                return rState;
            }
            long lastTxId = tnxId;
            if (fileState.getLastTnxId() > rState.getSnapshotTxId()) {
                DFSCloseFile closeFile = generateSnapshot(fileState, true, tnxId);
                MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(NameNodeEnv.get().source(),
                        closeFile,
                        DFSCloseFile.class,
                        entity.getDomain(),
                        entity.getEntity(),
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
        } catch (SnapshotError se) {
            throw se;
        } catch (Exception ex) {
            throw new SnapshotError(ex);
        } finally {
            stateManager.replicationLock().unlock();
        }
    }

    public static DFSCloseFile generateSnapshot(@NonNull DFSFileState state,
                                                @NonNull DFSFile file,
                                                long txId) throws Exception {
        DFSTransaction tx = DFSTransaction.newBuilder()
                .setOp(DFSTransaction.Operation.CLOSE)
                .setTransactionId(txId)
                .setTimestamp(System.currentTimeMillis())
                .build();

        DFSCloseFile.Builder builder = DFSCloseFile.newBuilder();
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

    public static DFSCloseFile generateSnapshot(@NonNull DFSFileState state,
                                                boolean addBlocks,
                                                long txId) throws Exception {
        DFSTransaction tx = DFSTransaction.newBuilder()
                .setOp(DFSTransaction.Operation.CLOSE)
                .setTransactionId(txId)
                .setTimestamp(System.currentTimeMillis())
                .build();

        DFSFile file = ProtoBufUtils.build(state);
        DFSCloseFile.Builder builder = DFSCloseFile.newBuilder();
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

        private MessagingConfig senderConfig;
        private MessagingConfig tnxSenderConfig;
        private long defaultLockTimeout = 5 * 60 * 1000;

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
            } catch (Exception ex) {
                throw new ConfigurationException(ex);
            }
        }
    }

    public static class SnapshotCallBack implements FilterAddCallback {
        private final HDFSSnapshotProcessor processor;

        public SnapshotCallBack(@NonNull HDFSSnapshotProcessor processor) {
            this.processor = processor;
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
            try {
                int count = processor.processFilterWithLock(filter, matcher.domain());
                DefaultLogger.LOG.info(String.format("Processed filer: [filter=%s][files=%d]",
                        JSONUtils.asString(filter.filter(), Filter.class), count));
            } catch (Exception ex) {
                DefaultLogger.LOG.error(String.format("Error processing filter: %s", filter.filter().toString()), ex);
                DefaultLogger.LOG.debug(DefaultLogger.stacktrace(ex));
            }
        }

        /**
         * @param matcher
         */
        @Override
        public void onStart(@NonNull DomainFilterMatcher matcher) {

        }
    }
}
