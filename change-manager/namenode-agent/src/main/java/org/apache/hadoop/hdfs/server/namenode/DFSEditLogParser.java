package org.apache.hadoop.hdfs.server.namenode;

import ai.sapper.hcdc.agents.common.DFSAgentError;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.model.DFSEditLogBatch;
import ai.sapper.hcdc.agents.model.DFSTransactionType;
import ai.sapper.hcdc.common.model.DFSFileRename;
import ai.sapper.hcdc.common.model.DFSTransaction;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdfs.protocol.Block;


@Getter
@Accessors(fluent = true)
public class DFSEditLogParser {
    /*
          OP_ADD                        ((byte)  0, AddOp.class),
          // deprecated operation
          OP_RENAME_OLD                 ((byte)  1, RenameOldOp.class),
          OP_DELETE                     ((byte)  2, DeleteOp.class),
          OP_MKDIR                      ((byte)  3, MkdirOp.class),
          OP_SET_REPLICATION            ((byte)  4, SetReplicationOp.class),
          @Deprecated OP_DATANODE_ADD   ((byte)  5), // obsolete
          @Deprecated OP_DATANODE_REMOVE((byte)  6), // obsolete
          OP_SET_PERMISSIONS            ((byte)  7, SetPermissionsOp.class),
          OP_SET_OWNER                  ((byte)  8, SetOwnerOp.class),
          OP_CLOSE                      ((byte)  9, CloseOp.class),
          OP_SET_GENSTAMP_V1            ((byte) 10, SetGenstampV1Op.class),
          OP_SET_NS_QUOTA               ((byte) 11, SetNSQuotaOp.class), // obsolete
          OP_CLEAR_NS_QUOTA             ((byte) 12, ClearNSQuotaOp.class), // obsolete
          OP_TIMES                      ((byte) 13, TimesOp.class), // set atime, mtime
          OP_SET_QUOTA                  ((byte) 14, SetQuotaOp.class),
          // filecontext rename
          OP_RENAME                     ((byte) 15, RenameOp.class),
          // concat files
          OP_CONCAT_DELETE              ((byte) 16, ConcatDeleteOp.class),
          OP_SYMLINK                    ((byte) 17, SymlinkOp.class),
          OP_GET_DELEGATION_TOKEN       ((byte) 18, GetDelegationTokenOp.class),
          OP_RENEW_DELEGATION_TOKEN     ((byte) 19, RenewDelegationTokenOp.class),
          OP_CANCEL_DELEGATION_TOKEN    ((byte) 20, CancelDelegationTokenOp.class),
          OP_UPDATE_MASTER_KEY          ((byte) 21, UpdateMasterKeyOp.class),
          OP_REASSIGN_LEASE             ((byte) 22, ReassignLeaseOp.class),
          OP_END_LOG_SEGMENT            ((byte) 23, EndLogSegmentOp.class),
          OP_START_LOG_SEGMENT          ((byte) 24, StartLogSegmentOp.class),
          OP_UPDATE_BLOCKS              ((byte) 25, UpdateBlocksOp.class),
          OP_CREATE_SNAPSHOT            ((byte) 26, CreateSnapshotOp.class),
          OP_DELETE_SNAPSHOT            ((byte) 27, DeleteSnapshotOp.class),
          OP_RENAME_SNAPSHOT            ((byte) 28, RenameSnapshotOp.class),
          OP_ALLOW_SNAPSHOT             ((byte) 29, AllowSnapshotOp.class),
          OP_DISALLOW_SNAPSHOT          ((byte) 30, DisallowSnapshotOp.class),
          OP_SET_GENSTAMP_V2            ((byte) 31, SetGenstampV2Op.class),
          OP_ALLOCATE_BLOCK_ID          ((byte) 32, AllocateBlockIdOp.class),
          OP_ADD_BLOCK                  ((byte) 33, AddBlockOp.class),
          OP_ADD_CACHE_DIRECTIVE        ((byte) 34, AddCacheDirectiveInfoOp.class),
          OP_REMOVE_CACHE_DIRECTIVE     ((byte) 35, RemoveCacheDirectiveInfoOp.class),
          OP_ADD_CACHE_POOL             ((byte) 36, AddCachePoolOp.class),
          OP_MODIFY_CACHE_POOL          ((byte) 37, ModifyCachePoolOp.class),
          OP_REMOVE_CACHE_POOL          ((byte) 38, RemoveCachePoolOp.class),
          OP_MODIFY_CACHE_DIRECTIVE     ((byte) 39, ModifyCacheDirectiveInfoOp.class),
          OP_SET_ACL                    ((byte) 40, SetAclOp.class),
          OP_ROLLING_UPGRADE_START      ((byte) 41, RollingUpgradeStartOp.class),
          OP_ROLLING_UPGRADE_FINALIZE   ((byte) 42, RollingUpgradeFinalizeOp.class),
          OP_SET_XATTR                  ((byte) 43, SetXAttrOp.class),
          OP_REMOVE_XATTR               ((byte) 44, RemoveXAttrOp.class),
          OP_SET_STORAGE_POLICY         ((byte) 45, SetStoragePolicyOp.class),
          OP_TRUNCATE                   ((byte) 46, TruncateOp.class),
          OP_APPEND                     ((byte) 47, AppendOp.class),
          OP_SET_QUOTA_BY_STORAGETYPE   ((byte) 48, SetQuotaByStorageTypeOp.class),
          OP_ADD_ERASURE_CODING_POLICY  ((byte) 49, AddErasureCodingPolicyOp.class),
          OP_ENABLE_ERASURE_CODING_POLICY((byte) 50, EnableErasureCodingPolicyOp.class),
          OP_DISABLE_ERASURE_CODING_POLICY((byte) 51,
              DisableErasureCodingPolicyOp.class),
          OP_REMOVE_ERASURE_CODING_POLICY((byte) 52, RemoveErasureCodingPolicyOp.class),
     */
    private long startTxId = -1;
    private long endTxId = -1;
    private NameNodeEnv env;

    public DFSEditLogParser withStartTxId(long startTxId) {
        this.startTxId = startTxId;
        return this;
    }

    public DFSEditLogParser withEndTxId(long endTxId) {
        this.endTxId = endTxId;
        return this;
    }

    public DFSEditLogParser withEnv(@NonNull NameNodeEnv env) {
        this.env = env;
        return this;
    }

    public DFSTransactionType<?> parse(@NonNull FSEditLogOp op, @NonNull DFSEditLogBatch batch) throws DFSAgentError {
        Preconditions.checkNotNull(env);
        if (op.opCode == FSEditLogOpCodes.OP_START_LOG_SEGMENT) {
            if (op.getTransactionId() != batch.startTnxId()) {
                throw new DFSAgentError(
                        String.format("Start transaction ID mismatch. [expected=%d][actual=%d]",
                                op.getTransactionId(), batch.startTnxId()));
            }
        } else if (op.opCode == FSEditLogOpCodes.OP_END_LOG_SEGMENT) {
            if (batch.isCurrent()) {
                batch.endTnxId(op.getTransactionId());
            } else {
                if (op.getTransactionId() != batch.endTnxId()) {
                    throw new DFSAgentError(
                            String.format("Start transaction ID mismatch. [expected=%d][actual=%d]",
                                    op.getTransactionId(), batch.endTnxId()));
                }
            }
        }
        switch (op.opCode) {
            case OP_ADD:
                return handleOpAdd(op, batch);
            case OP_ADD_BLOCK:
                return handleOpAddBlock(op, batch);
            case OP_APPEND:
                return handleOpAppend(op, batch);
            case OP_UPDATE_BLOCKS:
                return handleOpUpdateBlocks(op, batch);
            case OP_DELETE:
                return handleOpDelete(op, batch);
            case OP_TRUNCATE:
                return handleOpTruncate(op, batch);
            case OP_CLOSE:
                return handleOpClose(op, batch);
            case OP_RENAME:
                return handleOpRename(op, batch);
            case OP_RENAME_OLD:
                return handleOpRenameOld(op, batch);
            default:
                return handleDefault(op, batch);
        }
    }


    private boolean shouldLogTx(long txid) {
        if (startTxId > 0 && txid < startTxId) return false;
        if (endTxId > 0 && txid > endTxId) return false;
        return true;
    }

    private DFSTransactionType.DFSFileType fileType(@NonNull String path, long inodeId) {
        return new DFSTransactionType.DFSFileType()
                .namespace(env.source())
                .path(path)
                .inodeId(inodeId);
    }

    private DFSTransactionType<?> handleDefault(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        DFSTransactionType.DFSIgnoreTxType ift = new DFSTransactionType.DFSIgnoreTxType();
        ift.id(op.getTransactionId())
                .op(DFSTransaction.Operation.IGNORE);
        ift.namespace(env.source())
                .opCode(op.opCode.name());

        batch.transactions().add(ift);

        return ift;
    }

    private DFSTransactionType<?> handleOpRename(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.RenameOp) {
            FSEditLogOp.RenameOp rop = (FSEditLogOp.RenameOp) op;
            DFSTransactionType.DFSRenameFileType rft = new DFSTransactionType.DFSRenameFileType();

            rft.id(rop.txid)
                    .op(DFSTransaction.Operation.RENAME);
            rft.source(fileType(rop.src, -1))
                    .dest(fileType(rop.dst, -1));
            rft.length(rop.length);
            if (rop.options != null && rop.options.length > 0) {
                for (Options.Rename rn : rop.options) {
                    if (rn == Options.Rename.TO_TRASH) {
                        rft.opts(DFSFileRename.RenameOpts.TO_TRASH);
                        break;
                    } else if (rn == Options.Rename.OVERWRITE) {
                        rft.opts(DFSFileRename.RenameOpts.OVERWRITE);
                    }
                }
            }

            batch.transactions().add(rft);

            return rft;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.RenameOp.class, op.getClass()));
        }
    }

    private DFSTransactionType<?> handleOpRenameOld(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.RenameOldOp) {
            FSEditLogOp.RenameOldOp rop = (FSEditLogOp.RenameOldOp) op;
            DFSTransactionType.DFSRenameFileType rft = new DFSTransactionType.DFSRenameFileType();

            rft.id(rop.txid)
                    .op(DFSTransaction.Operation.RENAME);
            rft.source(fileType(rop.src, -1))
                    .dest(fileType(rop.dst, -1));
            rft.length(rop.length);

            batch.transactions().add(rft);

            return rft;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.RenameOp.class, op.getClass()));
        }
    }

    private DFSTransactionType<?> handleOpClose(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.CloseOp) {
            FSEditLogOp.CloseOp cop = (FSEditLogOp.CloseOp) op;
            DFSTransactionType.DFSCloseFileType cft = new DFSTransactionType.DFSCloseFileType();

            cft.id(cop.txid);
            cft.op(DFSTransaction.Operation.CLOSE)
                    .timestamp(cop.mtime);
            cft.file(fileType(cop.path, cop.inodeId));
            cft.length(cop.length)
                    .blockSize(cop.blockSize)
                    .modifiedTime(cop.mtime)
                    .accessedTime(cop.atime)
                    .overwrite(cop.overwrite);

            if (cop.blocks != null && cop.blocks.length > 0) {
                for (int ii = 0; ii < cop.blocks.length; ii++) {
                    DFSTransactionType.DFSBlockType bt = new DFSTransactionType.DFSBlockType();
                    bt.blockId(cop.blocks[ii].getBlockId());
                    bt.size(cop.blocks[ii].getNumBytes());
                    bt.generationStamp(cop.blocks[ii].getGenerationStamp());

                    cft.blocks().add(bt);
                }
            }
            batch.transactions().add(cft);

            return cft;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.CloseOp.class, op.getClass()));
        }
    }

    private DFSTransactionType<?> handleOpTruncate(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.TruncateOp) {
            FSEditLogOp.TruncateOp top = (FSEditLogOp.TruncateOp) op;
            DFSTransactionType.DFSTruncateBlockType tr = new DFSTransactionType.DFSTruncateBlockType();

            tr.id(top.txid)
                    .op(DFSTransaction.Operation.TRUNCATE)
                    .timestamp(top.timestamp);
            tr.file(fileType(top.src, -1));
            long trBlockId = -1;
            long trSize = 0;
            long trGStamp = -1;
            if (top.truncateBlock != null) {
                trBlockId = top.truncateBlock.getBlockId();
                trSize = top.truncateBlock.getNumBytes();
                trGStamp = top.truncateBlock.getGenerationStamp();
            }
            tr.block(new DFSTransactionType.DFSBlockType()
                    .blockId(trBlockId)
                    .size(trSize)
                    .generationStamp(trGStamp));
            tr.newLength(top.newLength);

            batch.transactions().add(tr);

            return tr;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.TruncateOp.class, op.getClass()));
        }
    }

    private DFSTransactionType<?> handleOpDelete(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.DeleteOp) {
            FSEditLogOp.DeleteOp dop = (FSEditLogOp.DeleteOp) op;
            DFSTransactionType.DFSDeleteFileType del = new DFSTransactionType.DFSDeleteFileType();

            del.id(dop.txid)
                    .op(DFSTransaction.Operation.DELETE)
                    .timestamp(dop.timestamp);
            del.file(fileType(dop.path, -1));

            batch.transactions().add(del);

            return del;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.DeleteOp.class, op.getClass()));
        }
    }

    private DFSTransactionType<?> handleOpUpdateBlocks(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.UpdateBlocksOp) {
            FSEditLogOp.UpdateBlocksOp ubop = (FSEditLogOp.UpdateBlocksOp) op;
            DFSTransactionType.DFSUpdateBlocksType upd = new DFSTransactionType.DFSUpdateBlocksType();

            upd.id(ubop.txid).op(DFSTransaction.Operation.UPDATE_BLOCKS);
            upd.file(fileType(ubop.path, -1));
            if (ubop.blocks != null && ubop.blocks.length > 0) {
                for (int ii = 0; ii < ubop.blocks.length; ii++) {
                    DFSTransactionType.DFSBlockType bt = new DFSTransactionType.DFSBlockType();
                    bt.blockId(ubop.blocks[ii].getBlockId());
                    bt.size(ubop.blocks[ii].getNumBytes());
                    bt.generationStamp(ubop.blocks[ii].getGenerationStamp());

                    upd.blocks().add(bt);
                }
            }

            batch.transactions().add(upd);

            return upd;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.UpdateBlocksOp.class, op.getClass()));
        }
    }

    private DFSTransactionType<?> handleOpAppend(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.AppendOp) {
            FSEditLogOp.AppendOp aop = (FSEditLogOp.AppendOp) op;
            DFSTransactionType.DFSAppendFileType aft = new DFSTransactionType.DFSAppendFileType();

            aft.id(aop.txid)
                    .op(DFSTransaction.Operation.APPEND);
            aft.file(fileType(aop.path, -1));
            aft.newBlock(aop.newBlock);

            batch.transactions().add(aft);

            return aft;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.AppendOp.class, op.getClass()));
        }
    }

    private DFSTransactionType<?> handleOpAddBlock(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.AddBlockOp) {
            FSEditLogOp.AddBlockOp abop = (FSEditLogOp.AddBlockOp) op;
            DFSTransactionType.DFSAddBlockType abt = new DFSTransactionType.DFSAddBlockType();

            abt.id(abop.txid)
                    .op(DFSTransaction.Operation.ADD_BLOCK);
            abt.file(fileType(abop.getPath(), -1));

            if (abop.getPenultimateBlock() != null) {
                Block pb = abop.getPenultimateBlock();
                DFSTransactionType.DFSBlockType bt = new DFSTransactionType.DFSBlockType();
                bt.blockId(pb.getBlockId());
                bt.size(pb.getNumBytes());
                bt.generationStamp(pb.getGenerationStamp());

                abt.penultimateBlock(bt);
            }
            if (abop.getLastBlock() != null) {
                Block lb = abop.getLastBlock();
                DFSTransactionType.DFSBlockType bt = new DFSTransactionType.DFSBlockType();
                bt.blockId(lb.getBlockId());
                bt.size(lb.getNumBytes());
                bt.generationStamp(lb.getGenerationStamp());

                abt.lastBlock(bt);
            }

            batch.transactions().add(abt);

            return abt;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.AddBlockOp.class, op.getClass()));
        }
    }

    /**
     * Operation notifies creation of new file Inode.
     *
     * @param op    - Add Operation
     * @param batch - Input Edit Log Batch
     * @throws DFSAgentError
     */
    private DFSTransactionType<?> handleOpAdd(FSEditLogOp op, DFSEditLogBatch batch) throws DFSAgentError {
        if (!shouldLogTx(op.txid)) return null;

        if (op instanceof FSEditLogOp.AddOp) {
            FSEditLogOp.AddOp aop = (FSEditLogOp.AddOp) op;
            DFSTransactionType.DFSAddFileType aft = new DFSTransactionType.DFSAddFileType();

            aft.id(aop.txid)
                    .op(DFSTransaction.Operation.ADD_FILE);
            aft.file(fileType(aop.path, aop.inodeId));
            aft.length(aop.length)
                    .blockSize(aop.blockSize)
                    .modifiedTime(aop.mtime)
                    .accessedTime(aop.atime)
                    .overwrite(aop.overwrite);

            if (aop.blocks != null && aop.blocks.length > 0) {
                for (int ii = 0; ii < aop.blocks.length; ii++) {
                    DFSTransactionType.DFSBlockType bt = new DFSTransactionType.DFSBlockType();
                    bt.blockId(aop.blocks[ii].getBlockId());
                    bt.size(aop.blocks[ii].getNumBytes());
                    bt.generationStamp(aop.blocks[ii].getGenerationStamp());
                    aft.blocks().add(bt);
                }
            }
            batch.transactions().add(aft);

            return aft;
        } else {
            throw new DFSAgentError(String.format("Invalid Edit Operation. [expected=%s][actual=%s]", FSEditLogOp.AddOp.class, op.getClass()));
        }
    }
}
