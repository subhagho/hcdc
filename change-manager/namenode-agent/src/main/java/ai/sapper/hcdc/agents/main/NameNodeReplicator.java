package ai.sapper.hcdc.agents.main;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.Service;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.model.LongTxState;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.NameNodeError;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.model.DFSFileState;
import ai.sapper.hcdc.agents.model.EBlockState;
import ai.sapper.hcdc.agents.model.EFileState;
import ai.sapper.hcdc.agents.model.ModuleTxState;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.PBImageXmlWriter;

import javax.naming.ConfigurationException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
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
    private ZkStateManager stateManager;
    private NameNodeEnv env;

    @Parameter(names = {"--imageDir", "-d"}, required = true, description = "Path to the directory containing FS Image file.")
    private String fsImageDir;
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configFile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    private EConfigFileType fileSource = EConfigFileType.File;
    @Parameter(names = {"--tmp"}, description = "Temp directory to use to create local files. [DEFAULT=System.getProperty(\"java.io.tmpdir\")]")
    private String tempDir = System.getProperty("java.io.tmpdir");

    private long txnId;
    private final Map<Long, DFSInode> inodes = new HashMap<>();
    private final Map<Long, DFSDirectory> directoryMap = new HashMap<>();

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
                td.mkdirs();
            }

            return this;
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            DefaultLogger.LOGGER.debug(DefaultLogger.stacktrace(t));
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

                    copy();

                    LongTxState nnTxState = (LongTxState) stateManager.initState(txnId);
                    ModuleTxState mTx = stateManager.updateReceivedTx(txnId);
                    mTx = stateManager.updateSnapshotTx(txnId);
                    DefaultLogger.info(env.LOG,
                            String.format("NameNode replication done. [state=%s][module state=%s]", nnTxState, mTx));

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
            DFSInode inode = inodes.get(id);
            if (inode.type == EInodeType.DIRECTORY) continue;
            DefaultLogger.debug(env.LOG, String.format("Copying HDFS file entry. [path=%s]", inode.path()));

            DFSFileState fileState = stateManager
                    .fileStateHelper()
                    .create(NameNodeEnv.get(name()).source(),
                            inode,
                            EFileState.Finalized,
                            txnId);
            if (inode.blocks != null && !inode.blocks.isEmpty()) {
                long prevBlockId = -1;
                for (DFSInodeBlock block : inode.blocks) {
                    fileState = stateManager
                            .fileStateHelper()
                            .addOrUpdateBlock(fileState.getFileInfo().getHdfsPath(),
                                    block.id,
                                    prevBlockId,
                                    inode.mTime,
                                    block.numBytes,
                                    block.genStamp,
                                    EBlockState.Finalized,
                                    txnId);
                    prevBlockId = block.id;
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
        txnId = Long.parseLong(s);

        List<HierarchicalConfiguration<ImmutableNode>> nodes = rootNode.configurationsAt(Constants.NODE_INODES);
        if (nodes != null && !nodes.isEmpty()) {
            for (HierarchicalConfiguration<ImmutableNode> node : nodes) {
                DFSInode inode = readInode(node);
                if (inode != null) {
                    inodes.put(inode.id, inode);
                }
            }
        }
        List<HierarchicalConfiguration<ImmutableNode>> dnodes = rootNode.configurationsAt(Constants.NODE_DIR_NODE);
        if (dnodes != null && !dnodes.isEmpty()) {
            for (HierarchicalConfiguration<ImmutableNode> node : dnodes) {
                DFSDirectory dir = new DFSDirectory().read(node);
                directoryMap.put(dir.id, dir);
            }
        }
        long rid = findRootInodeId();
        Preconditions.checkState(rid >= 0);
        findChildren(rid);
    }

    private void findChildren(long id) {
        if (directoryMap.containsKey(id)) {
            DFSInode inode = inodes.get(id);
            Preconditions.checkNotNull(inode);
            if (inode.children != null && !inode.children.isEmpty()) return;
            DFSDirectory dir = directoryMap.get(id);
            if (dir.children != null && !dir.children.isEmpty()) {
                for (long cid : dir.children) {
                    DFSInode cnode = inodes.get(cid);
                    Preconditions.checkNotNull(cnode);
                    inode.addChild(cnode);
                    findChildren(cnode.id);
                }
            }
        }
    }

    private long findRootInodeId() {
        for (long id : inodes.keySet()) {
            DFSInode inode = inodes.get(id);
            if (Strings.isNullOrEmpty(inode.name)) {
                return id;
            }
        }
        return -1L;
    }

    private DFSInode readInode(HierarchicalConfiguration<ImmutableNode> node) throws Exception {
        return new DFSInode().read(node);
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
            hdfsConnection = get().getString(CONFIG_CONNECTION_HDFS);
            if (Strings.isNullOrEmpty(hdfsConnection)) {
                throw new ConfigurationException(String.format("NameNode Replicator Configuration Error: missing [%s]", CONFIG_CONNECTION_HDFS));
            }
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

    public enum EInodeType {
        FILE, DIRECTORY, SYMLINK
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class DFSInode {
        private static final String NODE_INODE_ID = "id";
        private static final String NODE_INODE_TYPE = "type";
        private static final String NODE_INODE_NAME = "name";
        private static final String NODE_INODE_MTIME = "mtime";
        private static final String NODE_INODE_ATIME = "atime";
        private static final String NODE_INODE_BS = "preferredBlockSize";
        private static final String NODE_INODE_PERM = "permission";

        private static final String NODE_INODE_BLOCKS = "blocks";
        private static final String NODE_INODE_BLOCK = String.format("%s.block", NODE_INODE_BLOCKS);

        private long id;
        private EInodeType type;
        private String name;
        private long mTime;
        private long aTime;
        private long preferredBlockSize;
        private String user;
        private String group;
        private DFSInode parent = null;
        private Map<Long, DFSInode> children;

        private List<DFSInodeBlock> blocks;

        public DFSInode addChild(@NonNull DFSInode child) {
            if (children == null) {
                children = new HashMap<>();
            }
            children.put(child.id, child);
            child.parent = this;

            return this;
        }

        public String path() {
            if (parent == null) return "";
            String pp = parent.path();
            return String.format("%s/%s", pp, name);
        }

        public DFSInode read(@NonNull HierarchicalConfiguration<ImmutableNode> node) throws Exception {
            id = node.getLong(NODE_INODE_ID);
            String s = node.getString(NODE_INODE_TYPE);
            if (Strings.isNullOrEmpty(s)) {
                throw new Exception(String.format("Missing Inode field. [field=%s]", NODE_INODE_TYPE));
            }
            type = EInodeType.valueOf(s);
            name = node.getString(NODE_INODE_NAME);
            if (node.containsKey(NODE_INODE_MTIME)) {
                mTime = node.getLong(NODE_INODE_MTIME);
            }
            if (node.containsKey(NODE_INODE_ATIME)) {
                aTime = node.getLong(NODE_INODE_ATIME);
            }
            if (node.containsKey(NODE_INODE_BS)) {
                preferredBlockSize = node.getLong(NODE_INODE_BS);
            }
            s = node.getString(NODE_INODE_PERM);
            if (!Strings.isNullOrEmpty(s)) {
                String[] parts = s.split(":");
                if (parts.length == 3) {
                    user = parts[0];
                    group = parts[1];
                }
            }
            List<HierarchicalConfiguration<ImmutableNode>> nodes = node.configurationsAt(NODE_INODE_BLOCK);
            if (nodes != null && !nodes.isEmpty()) {
                blocks = new ArrayList<>();
                for (HierarchicalConfiguration<ImmutableNode> nn : nodes) {
                    DFSInodeBlock block = new DFSInodeBlock().read(nn);
                    blocks.add(block);
                }
                blocks.sort(new DFSInodeBlockComparator());
            }
            return this;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class DFSInodeBlock {
        private static final String NODE_BLOCK_ID = "id";
        private static final String NODE_BLOCK_NB = "numBytes";
        private static final String NODE_BLOCK_GEN = "genstamp";

        private long id;
        private long numBytes;
        private long genStamp;

        public DFSInodeBlock read(@NonNull HierarchicalConfiguration<ImmutableNode> node) throws Exception {
            id = node.getLong(NODE_BLOCK_ID);
            numBytes = node.getLong(NODE_BLOCK_NB);
            genStamp = node.getLong(NODE_BLOCK_GEN);

            return this;
        }
    }

    public static class DFSInodeBlockComparator implements Comparator<DFSInodeBlock> {

        /**
         * Compares its two arguments for order.  Returns a negative integer,
         * zero, or a positive integer as the first argument is less than, equal
         * to, or greater than the second.<p>
         * <p>
         * The implementor must ensure that {@code sgn(compare(x, y)) ==
         * -sgn(compare(y, x))} for all {@code x} and {@code y}.  (This
         * implies that {@code compare(x, y)} must throw an exception if and only
         * if {@code compare(y, x)} throws an exception.)<p>
         * <p>
         * The implementor must also ensure that the relation is transitive:
         * {@code ((compare(x, y)>0) && (compare(y, z)>0))} implies
         * {@code compare(x, z)>0}.<p>
         * <p>
         * Finally, the implementor must ensure that {@code compare(x, y)==0}
         * implies that {@code sgn(compare(x, z))==sgn(compare(y, z))} for all
         * {@code z}.<p>
         * <p>
         * It is generally the case, but <i>not</i> strictly required that
         * {@code (compare(x, y)==0) == (x.equals(y))}.  Generally speaking,
         * any comparator that violates this condition should clearly indicate
         * this fact.  The recommended language is "Note: this comparator
         * imposes orderings that are inconsistent with equals."<p>
         * <p>
         * In the foregoing description, the notation
         * {@code sgn(}<i>expression</i>{@code )} designates the mathematical
         * <i>signum</i> function, which is defined to return one of {@code -1},
         * {@code 0}, or {@code 1} according to whether the value of
         * <i>expression</i> is negative, zero, or positive, respectively.
         *
         * @param o1 the first object to be compared.
         * @param o2 the second object to be compared.
         * @return a negative integer, zero, or a positive integer as the
         * first argument is less than, equal to, or greater than the
         * second.
         * @throws NullPointerException if an argument is null and this
         *                              comparator does not permit null arguments
         * @throws ClassCastException   if the arguments' types prevent them from
         *                              being compared by this comparator.
         */
        @Override
        public int compare(DFSInodeBlock o1, DFSInodeBlock o2) {
            return (int) (o1.id - o2.id);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class DFSDirectory {
        private static final String NODE_DIR_ID = "parent";
        private static final String NODE_DIR_CHILD = "child";

        private long id;
        private List<Long> children;

        public DFSDirectory read(@NonNull HierarchicalConfiguration<ImmutableNode> node) throws Exception {
            id = node.getLong(NODE_DIR_ID);
            if (node.containsKey(NODE_DIR_CHILD)) {
                children = node.getList(Long.class, NODE_DIR_CHILD);
            }
            return this;
        }
    }
}
