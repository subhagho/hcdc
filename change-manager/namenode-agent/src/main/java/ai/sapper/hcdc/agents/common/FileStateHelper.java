package ai.sapper.hcdc.agents.common;

import ai.sapper.hcdc.common.model.DFSError;
import ai.sapper.hcdc.common.utils.JSONUtils;
import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.connections.ZookeeperConnection;
import ai.sapper.hcdc.core.model.*;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class FileStateHelper {
    private ZookeeperConnection connection;
    private String zkStatePath;

    public FileStateHelper withZkConnection(@NonNull ZookeeperConnection connection) {
        this.connection = connection;
        return this;
    }

    public FileStateHelper withZkPath(@NonNull String zkStatePath) {
        this.zkStatePath = zkStatePath;
        return this;
    }

    private synchronized void checkState() {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
    }

    public String getFilePath(String hdfsPath) {
        if (Strings.isNullOrEmpty(hdfsPath)) {
            return zkStatePath;
        } else {
            return PathUtils.formatZkPath(String.format("%s/%s", zkStatePath, hdfsPath));
        }
    }

    public DFSFileState create(@NonNull String path,
                               long inodeId,
                               long createdTime,
                               long blockSize,
                               @NonNull EFileState state,
                               long txId) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();

                String zp = getFilePath(path);
                DFSFileState fs = null;
                if (client.checkExists().forPath(zp) != null) {
                    fs = get(path);
                    if (!fs.checkDeleted()) {
                        throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                                path,
                                String.format("Valid File already exists. [path=%s]", path));
                    } else {
                        client.delete().forPath(zp);
                    }
                }
                fs = new DFSFileState();

                fs.setId(inodeId);
                fs.setZkPath(zp);
                fs.setHdfsFilePath(path);
                fs.setCreatedTime(createdTime);
                fs.setUpdatedTime(createdTime);
                fs.setBlockSize(blockSize);
                fs.setTimestamp(System.currentTimeMillis());
                fs.setLastTnxId(txId);
                fs.setState(state);

                byte[] data = JSONUtils.asBytes(fs, DFSFileState.class);
                client.create().creatingParentContainersIfNeeded().forPath(zp, data);
                return fs;
            } catch (Exception ex) {
                throw new StateManagerError(String.format("Error creating new file entry. [path=%s]", path));
            }
        }
    }

    public DFSFileState updateState(@NonNull String path, @NonNull EFileState state)
            throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                DFSFileState fs = get(path);
                if (fs == null) {
                    throw new StateManagerError(String.format("File state not found. [path=%s]", path));
                }
                fs.setState(state);
                return update(fs);
            } catch (Exception ex) {
                throw new StateManagerError(String.format("Error reading file entry. [path=%s]", path));
            }
        }
    }

    public DFSFileState updateState(@NonNull String path, long blockId, @NonNull EBlockState state)
            throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                DFSFileState fs = get(path);
                if (fs == null) {
                    throw new StateManagerError(String.format("File state not found. [path=%s]", path));
                }
                DFSBlockState bs = fs.get(blockId);
                if (bs == null) {
                    throw new StateManagerError(String.format("Block state not found. [path=%s][blockId=%d]", path, blockId));
                }
                bs.setState(state);

                return update(fs);
            } catch (Exception ex) {
                throw new StateManagerError(String.format("Error reading file entry. [path=%s]", path));
            }
        }
    }

    public DFSFileState addOrUpdateBlock(@NonNull String path,
                                         long blockId,
                                         long prevBlockId,
                                         long updatedTime,
                                         long dataSize,
                                         long generationStamp,
                                         @NonNull EBlockState state,
                                         long txId) throws StateManagerError, InvalidTransactionError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                DFSFileState fs = get(path);
                if (fs == null) {
                    throw new StateManagerError(String.format("File state not found. [path=%s]", path));
                }
                fs.setLastTnxId(txId);
                fs.setUpdatedTime(updatedTime);
                fs.setTimestamp(System.currentTimeMillis());
                long prevDataSize = 0;
                DFSBlockState bs = fs.get(blockId);
                if (bs == null) {
                    bs = new DFSBlockState();
                    bs.setPrevBlockId(prevBlockId);
                    bs.setBlockId(blockId);
                    bs.setCreatedTime(updatedTime);
                    bs.setBlockSize(fs.getBlockSize());
                    if (prevBlockId < 0) {
                        if (fs.hasBlocks()) {
                            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                                    fs.getHdfsFilePath(),
                                    String.format("Invalid Block Data: Previous Block ID not specified. [path=%s][blockID=%d]",
                                            fs.getHdfsFilePath(), bs.getBlockId()));
                        }
                    } else {
                        DFSBlockState pb = fs.get(prevBlockId);
                        if (pb == null) {
                            throw new InvalidTransactionError(DFSError.ErrorCode.SYNC_STOPPED,
                                    fs.getHdfsFilePath(),
                                    String.format("Invalid Block Data: Previous Block not found. [path=%s][blockID=%d]",
                                            fs.getHdfsFilePath(), bs.getBlockId()));
                        }
                    }
                    fs.add(bs);
                } else {
                    prevDataSize = bs.getDataSize();
                }
                bs.setUpdatedTime(updatedTime);
                bs.setLastTnxId(txId);
                bs.setDataSize(dataSize);
                bs.setGenerationStamp(generationStamp);
                bs.setState(state);
                BlockTnxDelta bd = new BlockTnxDelta();
                bd.setTnxId(txId);
                long soff = (prevDataSize > 0 ? prevDataSize - 1 : 0);
                long eoff = (dataSize > 0 ? dataSize - 1 : 0);
                bd.setStartOffset(soff);
                bd.setEndOffset(eoff);
                bd.setTimestamp(updatedTime);
                bs.add(bd);

                long ds = fs.getDataSize() + (dataSize - prevDataSize);
                fs.setDataSize(ds);

                return update(fs);
            } catch (InvalidTransactionError te) {
                throw te;
            } catch (Exception ex) {
                throw new StateManagerError(String.format("Error reading file entry. [path=%s]", path));
            }
        }
    }

    public DFSFileState update(@NonNull DFSFileState fileState) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileState.getHdfsFilePath()));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileState.getZkPath()));
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                String path = fileState.getZkPath();
                if (client.checkExists().forPath(path) == null) {
                    throw new StateManagerError(String.format("File record not found. [path=%s]", fileState.getHdfsFilePath()));
                }
                fileState.setTimestamp(System.currentTimeMillis());
                String json = JSONUtils.asString(fileState, DFSFileState.class);
                client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));

                return fileState;
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    public DFSFileState compact(@NonNull String hdfsPath, long age) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            DFSFileState fileState = get(hdfsPath);
            if (fileState == null) {
                throw new StateManagerError(String.format("File not found. [path=%s]", hdfsPath));
            }
            if (fileState.hasBlocks()) {
                for (DFSBlockState blockState : fileState.getBlocks()) {
                    if (blockState.hasTransactions()) {
                        List<BlockTnxDelta> array = new ArrayList<>();
                        long ts = System.currentTimeMillis() - age;
                        for (BlockTnxDelta tnx : blockState.getTransactions()) {
                            if (tnx.getTimestamp() > ts) {
                                array.add(tnx);
                            }
                        }
                        blockState.setTransactions(array);
                    }
                }
            }
            return update(fileState);
        }
    }

    public DFSFileState delete(@NonNull String hdfsPath) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hdfsPath));
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                DFSFileState fileState = get(hdfsPath);
                if (fileState == null) {
                    throw new StateManagerError(String.format("File state not found. [path=%s]", hdfsPath));
                }
                client.delete().deletingChildrenIfNeeded().forPath(fileState.getZkPath());

                return fileState;
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    public DFSFileState markDeleted(@NonNull String hdfsPath) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hdfsPath));
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                DFSFileState fstate = get(hdfsPath);
                if (fstate == null) {
                    throw new StateManagerError(String.format("File record data is NULL. [path=%s]", hdfsPath));
                }
                fstate.setState(EFileState.Deleted);

                return update(fstate);
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    public DFSFileState get(@NonNull String hdfsPath) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hdfsPath));

        try {
            CuratorFramework client = connection().client();
            String path = getFilePath(hdfsPath);
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    String json = new String(data, StandardCharsets.UTF_8);
                    return JSONUtils.read(json, DFSFileState.class);
                }
            }
            return null;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    public void deleteAll() throws StateManagerError {
        checkState();
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                String path = getFilePath(null);
                if (client.checkExists().forPath(path) != null) {
                    client.delete().deletingChildrenIfNeeded().forPath(path);
                }
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    public List<String> findFiles(@NonNull String hdfsPath) throws StateManagerError {
        checkState();
        try {
            CuratorFramework client = connection().client();
            String zpath = getFilePath(hdfsPath);
            List<String> paths = new ArrayList<>();
            search(zpath, paths, client);
            if (!paths.isEmpty()) return paths;
            return null;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    private void search(String zpath, List<String> paths, CuratorFramework client) throws Exception {
        List<String> children = client.getChildren().forPath(zpath);
        if (children != null && !children.isEmpty()) {
            for (String child : children) {
                String cp = PathUtils.formatZkPath(String.format("%s/%s", zpath, child));
                search(cp, paths, client);
            }
        } else {
            paths.add(zpath);
        }
    }
}
