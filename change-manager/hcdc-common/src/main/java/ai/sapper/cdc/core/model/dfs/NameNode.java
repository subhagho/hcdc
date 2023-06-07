package ai.sapper.cdc.core.model.dfs;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.*;

public class NameNode {

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
