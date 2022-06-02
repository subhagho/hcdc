package ai.sapper.hcdc.agents.namenode.model;

import ai.sapper.hcdc.agents.namenode.DFSAgentError;
import ai.sapper.hcdc.common.model.DFSAddBlock;
import ai.sapper.hcdc.common.model.DFSBlock;
import ai.sapper.hcdc.common.model.DFSFile;
import ai.sapper.hcdc.common.model.DFSTransaction;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class DFSLogTransaction<T> implements Comparable<DFSLogTransaction<T>> {
    private long id;
    private DFSTransaction.Operation op;
    private long timestamp;

    public DFSLogTransaction() {
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Compares this object with the specified object for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     *
     * <p>The implementor must ensure
     * {@code sgn(x.compareTo(y)) == -sgn(y.compareTo(x))}
     * for all {@code x} and {@code y}.  (This
     * implies that {@code x.compareTo(y)} must throw an exception iff
     * {@code y.compareTo(x)} throws an exception.)
     *
     * <p>The implementor must also ensure that the relation is transitive:
     * {@code (x.compareTo(y) > 0 && y.compareTo(z) > 0)} implies
     * {@code x.compareTo(z) > 0}.
     *
     * <p>Finally, the implementor must ensure that {@code x.compareTo(y)==0}
     * implies that {@code sgn(x.compareTo(z)) == sgn(y.compareTo(z))}, for
     * all {@code z}.
     *
     * <p>It is strongly recommended, but <i>not</i> strictly required that
     * {@code (x.compareTo(y)==0) == (x.equals(y))}.  Generally speaking, any
     * class that implements the {@code Comparable} interface and violates
     * this condition should clearly indicate this fact.  The recommended
     * language is "Note: this class has a natural ordering that is
     * inconsistent with equals."
     *
     * <p>In the foregoing description, the notation
     * {@code sgn(}<i>expression</i>{@code )} designates the mathematical
     * <i>signum</i> function, which is defined to return one of {@code -1},
     * {@code 0}, or {@code 1} according to whether the value of
     * <i>expression</i> is negative, zero, or positive, respectively.
     *
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    @Override
    public int compareTo(@NonNull DFSLogTransaction<T> o) {
        return (int) (id - o.id);
    }

    public DFSTransaction getProto() {
        return DFSTransaction.newBuilder().setTransactionId(id).setOp(op).setTimestamp(timestamp).build();
    }

    public void parseFrom(@NonNull DFSTransaction transaction) {
        id = transaction.getTransactionId();
        op = transaction.getOp();
        timestamp = transaction.getTimestamp();
    }

    public abstract T toProto() throws DFSAgentError;

    public abstract void parse(byte[] data) throws DFSAgentError;

    public abstract void parse(T proto) throws DFSAgentError;

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class DFSLogFile {
        private String path;
        private long inodeId;

        public DFSFile getProto() {
            return DFSFile.newBuilder().setPath(path).setInodeId(inodeId).build();
        }

        public void parse(@NonNull DFSFile file) {
            this.path = file.getPath();
            this.inodeId = file.getInodeId();
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class DFSLogBlock {
        private long blockId;
        private long size;
        private long generationStamp;

        public DFSBlock getProto() {
            return DFSBlock.newBuilder().setBlockId(blockId).setSize(size).setGenerationStamp(generationStamp).build();
        }

        public void parse(@NonNull DFSBlock block) {
            this.blockId = block.getBlockId();
            this.size = block.getSize();
            this.generationStamp = block.getGenerationStamp();
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class DFSLogAddBlock extends DFSLogTransaction<DFSAddBlock> {
        private DFSLogFile file;
        private DFSLogBlock penultimateBlock;
        private DFSLogBlock lastBlock;

        /**
         * @return
         * @throws DFSAgentError
         */
        @Override
        public DFSAddBlock toProto() throws DFSAgentError {
            Preconditions.checkNotNull(file);

            DFSAddBlock.Builder builder = DFSAddBlock.newBuilder();
            builder.setTransaction(getProto()).setFile(file.getProto());
            if (penultimateBlock != null) {
                builder.setPenultimateBlock(penultimateBlock.getProto());
            }
            if (lastBlock != null) {
                builder.setLastBlock(lastBlock.getProto());
            }
            return builder.build();
        }

        /**
         * @param data
         * @throws DFSAgentError
         */
        @Override
        public void parse(byte[] data) throws DFSAgentError {
            try {
                DFSAddBlock addBlock = DFSAddBlock.parseFrom(data);
                parse(addBlock);
            } catch (InvalidProtocolBufferException e) {
                throw new DFSAgentError(String.format("Error reading from byte array. [type=%s]", getClass().getCanonicalName()), e);
            }
        }

        /**
         * @param proto
         * @throws DFSAgentError
         */
        @Override
        public void parse(DFSAddBlock proto) throws DFSAgentError {
            Preconditions.checkArgument(proto.hasTransaction());
            Preconditions.checkArgument(proto.hasFile());

            parseFrom(proto.getTransaction());
            file = new DFSLogFile();
            file.parse(proto.getFile());

            if (proto.hasPenultimateBlock()) {
                penultimateBlock = new DFSLogBlock();
                penultimateBlock.parse(proto.getPenultimateBlock());
            }
            if (proto.hasLastBlock()) {
                lastBlock = new DFSLogBlock();
                lastBlock.parse(proto.getLastBlock());
            }
        }
    }
}
