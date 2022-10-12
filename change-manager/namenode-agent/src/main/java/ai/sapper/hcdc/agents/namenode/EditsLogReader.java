package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.messaging.ChangeDeltaSerDe;
import ai.sapper.cdc.core.messaging.MessageObject;
import ai.sapper.hcdc.agents.common.DFSEditsFileFinder;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.model.DFSEditLogBatch;
import ai.sapper.hcdc.agents.model.DFSTransactionType;
import ai.sapper.hcdc.agents.model.ModuleTxState;
import ai.sapper.hcdc.common.model.DFSChangeDelta;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.hdfs.tools.offlineEditsViewer.EditsLogFileReader;

import java.io.File;
import java.util.List;

import static ai.sapper.cdc.core.utils.TransactionLogger.LOGGER;

@Getter
@Accessors(fluent = true)
public class EditsLogReader extends HDFSEditsReader {
    private static final String PATH_NN_CURRENT_DIR = "current";

    private File editsDir;

    public EditsLogReader(@NonNull ZkStateManager stateManager, @NonNull String name) {
        super(name, stateManager);
    }

    @Override
    public EditsLogReader init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                               @NonNull ConnectionManager manger) throws ConfigurationException {
        try {
            processorConfig = new HDFSEditsReaderConfig(xmlConfig);
            processorConfig.read();

            setup(manger);

            String edir = NameNodeEnv.get(name).hadoopConfig().nameNodeEditsDir();
            editsDir = new File(edir);
            if (!editsDir.exists()) {
                throw new ConfigurationException(
                        String.format("Invalid Hadoop Configuration: Edits directory not found. [path=%s]",
                                editsDir.getAbsolutePath()));
            }

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public long doRun() throws Exception {
        ModuleTxState state = stateManager.getModuleState();
        EditsLogFileReader reader = new EditsLogFileReader();
        long txId = state.getReceivedTxId();
        if (txId < 0) {
            LOGGER.warn(getClass(), txId,
                    String.format("Name Node replication not initialized. [source=%s]",
                            NameNodeEnv.get(name).source()));
        }
        List<DFSEditsFileFinder.EditsLogFile> files = DFSEditsFileFinder
                .findEditsFiles(getPathNnCurrentDir(editsDir.getAbsolutePath()),
                        state.getReceivedTxId() + 1, -1);
        if (files != null && !files.isEmpty()) {
            for (DFSEditsFileFinder.EditsLogFile file : files) {
                LOGGER.debug(getClass(), state.getReceivedTxId(),
                        String.format("Reading edits file [path=%s][startTx=%d]",
                                file, state.getReceivedTxId()));
                try (DistributedLock lock = NameNodeEnv.get(name).globalLock()
                        .withLockTimeout(processorConfig().defaultLockTimeout())) {
                    if (DistributedLock.withRetry(lock, 5, 500)) {
                        try {
                            reader.run(file,
                                    state.getReceivedTxId(),
                                    file.endTxId(),
                                    NameNodeEnv.get(name));
                            DFSEditLogBatch batch = reader.batch();
                            if (batch.transactions() != null && !batch.transactions().isEmpty()) {
                                long tid = processBatch(batch, txId, stateManager.source());
                                if (tid > 0) {
                                    txId = tid;
                                    stateManager.update(txId);
                                    stateManager.updateReceivedTx(txId);
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
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

        return txId;
    }

    private long processBatch(DFSEditLogBatch batch, long lastTxId, String source) throws Exception {
        if (batch != null && batch.transactions() != null && !batch.transactions().isEmpty()) {
            long txid = -1;
            for (DFSTransactionType<?> tnx : batch.transactions()) {
                if (tnx.id() <= lastTxId) continue;
                Object proto = tnx.convertToProto();
                MessageObject<String, DFSChangeDelta> message = ChangeDeltaSerDe.create(proto,
                        proto.getClass(),
                        tnx.entity(source),
                        -1,
                        MessageObject.MessageMode.New);
                sender.send(message);
                txid = tnx.id();
            }
            return txid;
        }
        return -1;
    }

    private String getPathNnCurrentDir(String path) {
        return String.format("%s/%s", path, PATH_NN_CURRENT_DIR);
    }
}
