package ai.sapper.hcdc.agents.main;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.common.NameNodeError;
import ai.sapper.hcdc.agents.common.ZkStateManager;
import ai.sapper.hcdc.agents.model.NameNodeTxState;
import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.core.DistributedLock;
import ai.sapper.cdc.core.connections.HdfsConnection;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.DFSFileState;
import ai.sapper.cdc.core.model.EBlockState;
import ai.sapper.cdc.core.model.EFileState;
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
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.*;

@Getter
@Accessors(fluent = true)
public class NameNodeReplicator {
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

    @Parameter(names = {"--image", "-i"}, required = true, description = "Path to the FS Image file.")
    private String fsImageFile;
    @Parameter(names = {"--config", "-c"}, required = true, description = "Path to the configuration file.")
    private String configfile;
    @Parameter(names = {"--type", "-t"}, description = "Configuration file type. (File, Resource, Remote)")
    private String configSource;
    private EConfigFileType fileSource = EConfigFileType.File;
    @Parameter(names = {"--tmp"}, description = "Temp directory to use to create local files. [DEFAULT=System.getProperty(\"java.io.tmpdir\")]")
    private String tempDir = System.getProperty("java.io.tmpdir");

    private long txnId;
    private final Map<Long, DFSInode> inodes = new HashMap<>();
    private final Map<Long, DFSDirectory> directoryMap = new HashMap<>();

    public void init() throws NameNodeError {
        try {
            Preconditions.checkState(!Strings.isNullOrEmpty(configfile));
            if (!org.apache.parquet.Strings.isNullOrEmpty(configSource)) {
                fileSource = EConfigFileType.parse(configSource);
            }
            Preconditions.checkNotNull(fileSource);
            config = ConfigReader.read(configfile, fileSource);
            NameNodeEnv.setup(config);
            replicatorConfig = new ReplicatorConfig(config);
            replicatorConfig.read();

            hdfsConnection = NameNodeEnv.connectionManager()
                    .getConnection(replicatorConfig().hdfsConnection(), HdfsConnection.class);
            Preconditions.checkNotNull(hdfsConnection);
            hdfsConnection.connect();

            zkConnection = NameNodeEnv.stateManager().connection();
            Preconditions.checkNotNull(zkConnection);
            if (!zkConnection.isConnected()) zkConnection.connect();

            fs = hdfsConnection.fileSystem();
            stateManager = NameNodeEnv.stateManager();
            Preconditions.checkNotNull(stateManager);

        } catch (Throwable t) {
            DefaultLogger.LOG.error(t.getLocalizedMessage());
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            throw new NameNodeError(t);
        }
    }

    public void run() throws NameNodeError {
        try {
            try (DistributedLock lock = NameNodeEnv.globalLock()) {
                lock.lock();
                try {
                    String output = generateFSImageSnapshot();
                    DefaultLogger.LOG.info(String.format("Generated FS Image XML. [path=%s]", output));
                    readFSImageXml(output);
                    DefaultLogger.LOG.warn(
                            String.format("WARNING: Will delete existing file structure, if present. [path=%s]",
                                    stateManager.fileStateHelper().getFilePath(null)));
                    stateManager.deleteAll();
                    copy();
                    NameNodeTxState txState = stateManager.initState(txnId);
                    String tp = stateManager.updateSnapshotTxId(txnId);

                    DefaultLogger.LOG.info(String.format("NameNode replication done. [state=%s][TXID PATH=%s]", txState, tp));
                } finally {
                    lock.unlock();
                }
            }
            NameNodeEnv.dispose();
        } catch (Throwable t) {
            DefaultLogger.LOG.error(t.getLocalizedMessage());
            DefaultLogger.LOG.debug(DefaultLogger.stacktrace(t));
            throw new NameNodeError(t);
        }
    }

    private void copy() throws Exception {
        for (long id : inodes.keySet()) {
            DFSInode inode = inodes.get(id);
            if (inode.type == EInodeType.DIRECTORY) continue;
            DefaultLogger.LOG.debug(String.format("Copying HDFS file entry. [path=%s]", inode.path()));

            DFSFileState fileState = stateManager
                    .fileStateHelper()
                    .create(inode.path(),
                            inode.id,
                            inode.mTime,
                            inode.preferredBlockSize,
                            EFileState.Finalized,
                            txnId,
                            true);
            if (inode.blocks != null && !inode.blocks.isEmpty()) {
                long prevBlockId = -1;
                for (DFSInodeBlock block : inode.blocks) {
                    fileState = stateManager
                            .fileStateHelper()
                            .addOrUpdateBlock(fileState.getHdfsFilePath(),
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
            if (DefaultLogger.LOG.isDebugEnabled()) {
                String json = JSONUtils.asString(fileState, DFSFileState.class);
                DefaultLogger.LOG.debug(json);
            }
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

    private String generateFSImageSnapshot() throws Exception {
        File fsImage = new File(fsImageFile);
        Preconditions.checkState(fsImage.exists());

        String output = String.format("%s/%s.xml", tempDir, fsImage.getName());
        Configuration conf = new Configuration();
        try (PrintStream out = new PrintStream(output)) {
            new PBImageXmlWriter(conf, out).visit(new RandomAccessFile(fsImage.getAbsolutePath(),
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
            replicator.run();
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
