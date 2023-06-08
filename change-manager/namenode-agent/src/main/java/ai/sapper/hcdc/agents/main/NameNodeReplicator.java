package ai.sapper.hcdc.agents.main;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.NameNodeEnv;
import ai.sapper.cdc.core.NameNodeError;
import ai.sapper.cdc.core.Service;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.model.EFileState;
import ai.sapper.cdc.core.model.HCdcProcessingState;
import ai.sapper.cdc.core.model.HCdcTxId;
import ai.sapper.cdc.core.model.dfs.DFSFileState;
import ai.sapper.cdc.core.model.dfs.EBlockState;
import ai.sapper.cdc.core.model.dfs.NameNode;
import ai.sapper.cdc.core.state.HCdcStateManager;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter;

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
public class NameNodeReplicator implements Service<NameNodeEnv.ENameNodeEnvState> {
    private static final String FS_IMAGE_REGEX = "fsimage_(\\d+)$";

    private static class Constants {
        private static final String NODE_INODES = "INodeSection.inode";
        private static final String NODE_TX_ID = "NameSection.txid";
        private static final String NODE_DIR_SECTION = "INodeDirectorySection";
        private static final String NODE_DIR_NODE = String.format("%s.directory", NODE_DIR_SECTION);
    }

    private ZookeeperConnection zkConnection;
    private HdfsConnection hdfsConnection;
    private FileSystem fs;
    private HierarchicalConfiguration<ImmutableNode> config;
    private ReplicatorConfig replicatorConfig;
    private HCdcStateManager stateManager;
    private NameNodeEnv env;

    @Parameter(names = {"--imageDir", "-d"}, required = true, description = "Path to the directory containing FS Image file.")
    private String fsImageDir;
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    private EConfigFileType fileSource = EConfigFileType.File;
    @Parameter(names = {"--tmp"}, description = "Temp directory to use to create local files. [DEFAULT=System.getProperty(\"java.io.tmpdir\")]")
    private String tempDir = String.format("%s/hadoop/cdc", System.getProperty("java.io.tmpdir"));

    private HCdcTxId txnId;
    private final Map<Long, NameNode.DFSInode> inodes = new HashMap<>();
    private final Map<Long, NameNode.DFSDirectory> directoryMap = new HashMap<>();

    public NameNodeReplicator withFsImageDir(@NonNull String path) {
        fsImageDir = path;
        return this;
    }

    public NameNodeReplicator withTmpDir(@NonNull String tmpDir) {
        this.tempDir = tmpDir;
        return this;
    }

    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> setConfigFile(@NonNull String path) {
        configFile = path;
        return this;
    }

    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> setConfigSource(@NonNull String type) {
        configSource = type;
        return this;
    }

    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> init() throws NameNodeError {
        try {
            Preconditions.checkState(!Strings.isNullOrEmpty(configFile));
            if (!org.apache.parquet.Strings.isNullOrEmpty(configSource)) {
                fileSource = EConfigFileType.parse(configSource);
            }
            Preconditions.checkNotNull(fileSource);
            config = ConfigReader.read(configFile, fileSource);
            env = NameNodeEnv.setup(name(), getClass(), config);
            replicatorConfig = new ReplicatorConfig(env.rootConfig());
            replicatorConfig.read();

            hdfsConnection = NameNodeEnv.get(name()).connectionManager()
                    .getConnection(getReplicatorConfig().hdfsConnection(), HdfsConnection.class);
            Preconditions.checkNotNull(hdfsConnection);
            hdfsConnection.connect();

            zkConnection = NameNodeEnv.get(name()).stateManager().connection();
            Preconditions.checkNotNull(zkConnection);
            if (!zkConnection.isConnected()) zkConnection.connect();

            fs = hdfsConnection.fileSystem();
            stateManager = NameNodeEnv.get(name()).stateManager();
            Preconditions.checkNotNull(stateManager);

            File td = new File(tempDir);
            if (!td.exists()) {
                if (!td.mkdirs()) {
                    throw new Exception(String.format("Failed to create temp directory. [%s]", tempDir));
                }
            }

            return this;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            throw new NameNodeError(t);
        }
    }

    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> start() throws Exception {
        try {
            run();
            return this;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(env.LOG, t);
            DefaultLogger.error(env.LOG, t.getLocalizedMessage());
            NameNodeEnv.get(name()).error(t);
            throw t;
        }
    }

    @Override
    public Service<NameNodeEnv.ENameNodeEnvState> stop() throws Exception {
        NameNodeEnv.dispose(name());
        return this;
    }

    @Override
    public NameNodeEnv.NameNodeEnvState status() {
        try {
            return NameNodeEnv.status(name());
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public String name() {
        return getClass().getSimpleName();
    }

    private void run() throws NameNodeError {
        try {
            try (DistributedLock lock = NameNodeEnv.get(name()).globalLock()) {
                lock.lock();
                try {
                    String output = generateFSImageSnapshot();
                    DefaultLogger.info(env.LOG, String.format("Generated FS Image XML. [path=%s]", output));
                    readFSImageXml(output);
                    DefaultLogger.warn(env.LOG,
                            String.format("WARNING: Will delete existing file structure, if present. [path=%s]",
                                    stateManager.fileStateHelper().getFilePath(null)));
                    stateManager.deleteAll();
                    HCdcProcessingState processingState = (HCdcProcessingState) stateManager.initState(txnId);

                    copy();
                    processingState.updateProcessedTxId(txnId.getId())
                            .updateProcessedRecordId(txnId.getId(), txnId.getRecordId())
                            .updateSnapshotTxId(txnId.getId())
                            .updateSnapshotSequence(txnId.getId(), txnId.getRecordId());
                    processingState = (HCdcProcessingState) stateManager.update(processingState);
                    DefaultLogger.info(env.LOG,
                            String.format("NameNode replication done. [state=%s]", processingState));

                } finally {
                    lock.unlock();
                }
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(env.LOG, t);
            DefaultLogger.error(env.LOG, String.format("FATAL ERROR: %s", t.getLocalizedMessage()));
            throw new NameNodeError(t);
        }
    }

    private void copy() throws Exception {
        for (long id : inodes.keySet()) {
            NameNode.DFSInode inode = inodes.get(id);
            if (inode.type() == NameNode.EInodeType.DIRECTORY) continue;
            DefaultLogger.debug(env.LOG, String.format("Copying HDFS file entry. [path=%s]", inode.path()));

            DFSFileState fileState = stateManager
                    .fileStateHelper()
                    .create(NameNodeEnv.get(name()).source(),
                            inode,
                            EFileState.Finalized,
                            txnId.getId());
            if (inode.blocks() != null && !inode.blocks().isEmpty()) {
                long prevBlockId = -1;
                for (NameNode.DFSInodeBlock block : inode.blocks()) {
                    fileState = stateManager
                            .fileStateHelper()
                            .addOrUpdateBlock(fileState.getFileInfo().getHdfsPath(),
                                    block.id(),
                                    prevBlockId,
                                    inode.mTime(),
                                    block.numBytes(),
                                    block.genStamp(),
                                    EBlockState.Finalized,
                                    txnId.getId());
                    prevBlockId = block.id();
                }
            }
            NameNodeEnv.audit(name(), getClass(), fileState);
        }
    }

    private void readFSImageXml(String file) throws Exception {
        HierarchicalConfiguration<ImmutableNode> rootNode = ConfigReader.read(file, EConfigFileType.File);
        String s = rootNode.getString(Constants.NODE_TX_ID);
        if (Strings.isNullOrEmpty(s)) {
            throw new NameNodeError(String.format("NameNode Last Transaction ID not found. [file=%s]", file));
        }
        long tid = Long.parseLong(s);
        txnId = new HCdcTxId(tid);

        List<HierarchicalConfiguration<ImmutableNode>> nodes = rootNode.configurationsAt(Constants.NODE_INODES);
        if (nodes != null && !nodes.isEmpty()) {
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                NameNode.DFSInode inode = readInode(node);
                if (inode != null) {
                    inodes.put(inode.id(), inode);
                }
            }
        }
        List<HierarchicalConfiguration<ImmutableNode>> dnodes = rootNode.configurationsAt(Constants.NODE_DIR_NODE);
        if (dnodes != null && !dnodes.isEmpty()) {
            for (HierarchicalConfiguration<ImmutableNode> node : dnodes) {
                NameNode.DFSDirectory dir = new NameNode.DFSDirectory().read(node);
                directoryMap.put(dir.id(), dir);
            }
        }
        long rid = findRootInodeId();
        Preconditions.checkState(rid >= 0);
        findChildren(rid);
    }

    private void findChildren(long id) {
        if (directoryMap.containsKey(id)) {
            NameNode.DFSInode inode = inodes.get(id);
            Preconditions.checkNotNull(inode);
            if (inode.children() != null && !inode.children().isEmpty()) return;
            NameNode.DFSDirectory dir = directoryMap.get(id);
            if (dir.children() != null && !dir.children().isEmpty()) {
                for (long cid : dir.children()) {
                    NameNode.DFSInode cnode = inodes.get(cid);
                    Preconditions.checkNotNull(cnode);
                    inode.addChild(cnode);
                    findChildren(cnode.id());
                }
            }
        }
    }

    private long findRootInodeId() {
        for (long id : inodes.keySet()) {
            NameNode.DFSInode inode = inodes.get(id);
            if (Strings.isNullOrEmpty(inode.name())) {
                return id;
            }
        }
        return -1L;
    }

    private NameNode.DFSInode readInode(HierarchicalConfiguration<ImmutableNode> node) throws Exception {
        return new NameNode.DFSInode().read(node);
    }

    private File findImageFile(File dir) throws Exception {
        File[] files = dir.listFiles(new FilenameFilter() {
            private final Pattern pattern = Pattern.compile(FS_IMAGE_REGEX);

            @Override
            public boolean accept(File file, String s) {
                Matcher m = pattern.matcher(s);
                if (m.matches()) {
                    return !s.endsWith(".md5");
                }
                return false;
            }
        });
        if (files != null && files.length > 0) {
            long ts = 0;
            File latest = null;
            for (File file : files) {
                DefaultLogger.info(env.LOG, String.format("Found FS Image File: %s", file.getAbsolutePath()));
                Path path = Paths.get(file.toURI());
                BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
                FileTime ft = attr.lastModifiedTime();
                long t = ft.toMillis();
                if (t > ts) {
                    ts = t;
                    latest = file;
                }
            }
            return latest;
        }
        return null;
    }

    private String generateFSImageSnapshot() throws Exception {
        File fsImageDir = new File(this.fsImageDir);
        Preconditions.checkState(fsImageDir.exists());
        File fsImageFile = findImageFile(fsImageDir);
        if (fsImageFile == null) {
            throw new Exception(String.format("No FS Image file found. [path=%s]", fsImageDir.getAbsolutePath()));
        }
        String output = String.format("%s/%s.xml", tempDir, fsImageFile.getName());
        Configuration conf = new Configuration();
        try (PrintStream out = new PrintStream(output)) {
            new PBImageXmlWriter(conf, out).visit(new RandomAccessFile(fsImageFile.getAbsolutePath(),
                    "r"));
        }

        return output;
    }

    @Getter
    @Accessors(fluent = true)
    public static class ReplicatorConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "replicator";
        private static final String CONFIG_CONNECTION_HDFS = "connections.hdfs";

        private String hdfsConnection;

        public ReplicatorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public ReplicatorConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String configPath) {
            super(config, configPath);
        }

        public void read() throws ConfigurationException {
            hdfsConnection = read(CONFIG_CONNECTION_HDFS);
        }
    }

    public static void main(String[] args) {
        try {
            NameNodeReplicator replicator = new NameNodeReplicator();
            JCommander.newBuilder().addObject(replicator).build().parse(args);
            replicator.init();
            replicator.start();
            replicator.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}
