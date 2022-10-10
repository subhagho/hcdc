package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.StateManagerError;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.model.BlockTransactionDelta;
import ai.sapper.hcdc.agents.main.NameNodeReplicator;
import ai.sapper.hcdc.agents.model.*;
import ai.sapper.hcdc.common.model.DFSError;
import ai.sapper.hcdc.common.model.DFSFile;
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
            return new PathUtils.ZkPathBuilder(zkStatePath)
                    .withPath(hdfsPath)
                    .build();
        }
    }

    public DFSFileState create(@NonNull DFSFile file,
                               long createdTime,
                               long blockSize,
                               @NonNull EFileState state,
                               long txId) throws StateManagerError {
        return create(file, createdTime, blockSize, state, txId, false);
    }

    public DFSFileState create(@NonNull DFSFile file,
                               long createdTime,
                               long blockSize,
                               @NonNull EFileState state,
                               long txId,
                               boolean updateIfExists) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        String path = file.getEntity().getEntity();
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                String zp = getFilePath(path);
                DFSFileState fs = null;
                if (client.checkExists().forPath(zp) != null) {
                    fs = get(path);
                    if (!updateIfExists) {
                        if (!fs.checkDeleted()) {
                            throw new InvalidTransactionError(txId,
                                    DFSError.ErrorCode.SYNC_STOPPED,
                                    path,
                                    new Exception(String.format("Valid File already exists. [path=%s]",
                                            path)))
                                    .withFile(file);
                        } else {
                            client.delete().forPath(zp);
                        }
                    }
                }
                if (fs == null) {
                    fs = new DFSFileState();
                }
                fs.setFileInfo(new DFSFileInfo().parse(file));
                fs.setZkPath(zp);
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

    public DFSFileState create(@NonNull String namespace,
                               @NonNull NameNodeReplicator.DFSInode inode,
                               @NonNull EFileState state,
                               long txId) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();

                String zp = getFilePath(inode.path());
                DFSFileState fs = null;
                if (client.checkExists().forPath(zp) != null) {
                    fs = get(inode.path());
                    if (!fs.checkDeleted()) {
                        throw new InvalidTransactionError(txId,
                                DFSError.ErrorCode.SYNC_STOPPED,
                                inode.path(),
                                new Exception(String.format("Valid File already exists. [path=%s]", inode.path())));
                    } else {
                        client.delete().forPath(zp);
                    }

                }
                if (fs == null) {
                    fs = new DFSFileState();
                }
                DFSFileInfo fi = new DFSFileInfo();
                fi.setNamespace(namespace);
                fi.setHdfsPath(inode.path());
                fi.setInodeId(inode.id());
                fs.setFileInfo(fi);
                fs.setZkPath(zp);
                fs.setCreatedTime(inode.mTime());
                fs.setUpdatedTime(inode.mTime());
                fs.setBlockSize(inode.preferredBlockSize());
                fs.setTimestamp(System.currentTimeMillis());
                fs.setLastTnxId(txId);
                fs.setState(state);

                byte[] data = JSONUtils.asBytes(fs, DFSFileState.class);
                client.create().creatingParentContainersIfNeeded().forPath(zp, data);
                return fs;
            } catch (Exception ex) {
                throw new StateManagerError(String.format("Error creating new file entry. [path=%s]", inode.path()));
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
                            throw new InvalidTransactionError(txId,
                                    DFSError.ErrorCode.SYNC_STOPPED,
                                    fs.getFileInfo().getHdfsPath(),
                                    new Exception(String.format(
                                            "Invalid Block Data: Previous Block ID not specified. [path=%s][blockID=%d]",
                                            fs.getFileInfo().getHdfsPath(), bs.getBlockId())));
                        }
                    } else {
                        DFSBlockState pb = fs.get(prevBlockId);
                        if (pb == null) {
                            throw new InvalidTransactionError(txId,
                                    DFSError.ErrorCode.SYNC_STOPPED,
                                    fs.getFileInfo().getHdfsPath(),
                                    new Exception(String.format(
                                            "Invalid Block Data: Previous Block not found. [path=%s][blockID=%d]",
                                            fs.getFileInfo().getHdfsPath(), bs.getBlockId())));
                        }
                    }
                    fs.add(bs);
                } else {
                    prevDataSize = bs.getDataSize();
                }
                if (!bs.hasTransaction(txId)) {
                    bs.setUpdatedTime(updatedTime);
                    bs.setLastTnxId(txId);
                    bs.setDataSize(dataSize);
                    bs.setGenerationStamp(generationStamp);
                    bs.setState(state);
                    BlockTransactionDelta bd = new BlockTransactionDelta();
                    bd.setTnxId(txId);
                    long soff = (prevDataSize > 0 ? prevDataSize : 0);
                    long eoff = (dataSize > 0 ? dataSize - 1 : 0);
                    bd.setStartOffset(soff);
                    bd.setEndOffset(eoff);
                    bd.setTimestamp(updatedTime);
                    bs.add(bd);

                    long ds = fs.getDataSize() + (dataSize - prevDataSize);
                    fs.setDataSize(ds);
                }

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
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileState.getFileInfo().getHdfsPath()));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileState.getZkPath()));
        synchronized (this) {
            try {
                CuratorFramework client = connection().client();
                String path = fileState.getZkPath();
                if (client.checkExists().forPath(path) == null) {
                    throw new StateManagerError(
                            String.format("File record not found. [path=%s]",
                                    fileState.getFileInfo().getHdfsPath()));
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
                        List<BlockTransactionDelta> array = new ArrayList<>();
                        long ts = System.currentTimeMillis() - age;
                        for (BlockTransactionDelta tnx : blockState.getTransactions()) {
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
                if (fileState != null) {
                    client.delete().forPath(fileState.getZkPath());
                    DefaultLogger.LOGGER.debug(String.format("Deleted file: [path=%s]", hdfsPath));
                } else if (checkIsDirectoryPath(hdfsPath)) {
                    String path = getFilePath(hdfsPath);
                    client.delete().deletingChildrenIfNeeded().forPath(path);
                    DefaultLogger.LOGGER.debug(String.format("Deleted directory: [path=%s]", hdfsPath));
                }
                return fileState;
            } catch (Exception ex) {
                throw new StateManagerError(ex);
            }
        }
    }

    public DFSFileState markDeleted(@NonNull String hdfsPath, boolean reset) throws StateManagerError {
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
                if (reset)
                    fstate.reset();
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

    public List<DFSFileState> listFiles(String base) throws StateManagerError {
        return listFiles(base, null);
    }

    public List<DFSFileState> listFiles(String base, EFileState fileState) throws StateManagerError {
        checkState();
        try {
            CuratorFramework client = connection().client();
            String path = getFilePath(base);
            if (client.checkExists().forPath(path) != null) {
                List<DFSFileState> files = new ArrayList<>();
                listFiles(path, files, client, fileState);
                if (!files.isEmpty()) return files;
            }
            return null;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }

    private void listFiles(String path,
                           List<DFSFileState> files,
                           CuratorFramework client,
                           EFileState fileState) throws Exception {
        List<String> children = client.getChildren().forPath(path);
        if (children == null || children.isEmpty()) {
            byte[] data = client.getData().forPath(path);
            if (data != null && data.length > 0) {
                DFSFileState fs = JSONUtils.read(data, DFSFileState.class);
                if (fileState == null || fs.getState() == fileState)
                    files.add(fs);
            }
        } else {
            for (String child : children) {
                String cpath = new PathUtils.ZkPathBuilder(path)
                        .withPath(child)
                        .build();
                listFiles(cpath, files, client, fileState);
            }
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
                String cp = new PathUtils.ZkPathBuilder(zpath)
                        .withPath(child)
                        .build();
                search(cp, paths, client);
            }
        } else {
            paths.add(zpath);
        }
    }

    public boolean checkIsDirectoryPath(String hdfsPath) throws StateManagerError {
        Preconditions.checkNotNull(connection);
        Preconditions.checkState(connection.isConnected());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(hdfsPath));

        try {
            CuratorFramework client = connection().client();
            String path = getFilePath(hdfsPath);
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    DefaultLogger.LOGGER.debug(
                            String.format("Specified path is a file. [path=%s]", hdfsPath));
                    return false;
                }
                DefaultLogger.LOGGER.debug(
                        String.format("Specified path found. [path=%s]", hdfsPath));
                return true;
            }
            return false;
        } catch (Exception ex) {
            throw new StateManagerError(ex);
        }
    }
}
