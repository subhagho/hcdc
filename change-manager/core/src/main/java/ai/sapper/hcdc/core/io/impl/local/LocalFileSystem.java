package ai.sapper.hcdc.core.io.impl.local;

import ai.sapper.hcdc.common.utils.PathUtils;
import ai.sapper.hcdc.core.io.FileSystem;
import ai.sapper.hcdc.core.io.PathInfo;
import ai.sapper.hcdc.core.io.Reader;
import ai.sapper.hcdc.core.io.Writer;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LocalFileSystem extends FileSystem {
    private FileSystemConfig fsConfig = null;

    /**
     * @param config
     * @param pathPrefix
     * @return
     * @throws IOException
     */
    @Override
    public FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config, String pathPrefix) throws IOException {
        try {
            if (fsConfig == null) {
                fsConfig = new LocalFileSystemConfig(config, pathPrefix);
            }
            fsConfig.read();
            LocalPathInfo rp = new LocalPathInfo(fsConfig.rootPath(), "");
            setRootPath(rp);

            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public PathInfo get(@NonNull String path, String domain) throws IOException {
        if (root() != null) {
            path = PathUtils.formatPath(String.format("%s/%s/%s", root().path(), domain, path));
        }
        return new LocalPathInfo(path, domain);
    }

    /**
     * @param path
     * @param domain
     * @param prefix
     * @return
     * @throws IOException
     */
    @Override
    public PathInfo get(@NonNull String path, String domain, boolean prefix) throws IOException {
        if (prefix) {
            return get(path, domain);
        }
        return new LocalPathInfo(path, domain);
    }

    /**
     * @param config
     * @return
     */
    @Override
    public PathInfo get(@NonNull Map<String, String> config) {
        return new LocalPathInfo(config);
    }


    /**
     * @param path
     * @param name
     * @return
     * @throws IOException
     */
    @Override
    public String mkdir(@NonNull PathInfo path, @NonNull String name) throws IOException {
        LocalPathInfo di = new LocalPathInfo(String.format("%s/%s", path.path(), name), path.domain());
        if (!di.exists()) {
            if (!di.file().mkdir()) {
                throw new IOException(String.format("Failed to create directory. [path=%s]", di.file().getAbsolutePath()));
            }
        }
        return di.file().getAbsolutePath();
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public String mkdirs(@NonNull PathInfo path) throws IOException {
        LocalPathInfo di = new LocalPathInfo(path.path(), path.domain());
        if (!di.exists()) {
            if (!di.file().mkdirs()) {
                throw new IOException(String.format("Failed to create directory. [path=%s]", di.file().getAbsolutePath()));
            }
        }
        return di.file().getAbsolutePath();
    }

    /**
     * @param source
     * @param directory
     * @throws IOException
     */
    @Override
    public PathInfo upload(@NonNull File source, @NonNull PathInfo directory) throws IOException {
        Preconditions.checkArgument(directory.isDirectory());
        File dest = new File(String.format("%s/%s", directory.path(), FilenameUtils.getName(source.getAbsolutePath())));
        FileUtils.copyFile(source, dest);

        return get(dest.getAbsolutePath(), directory.domain(), false);
    }

    /**
     * @param path
     * @param recursive
     * @return
     * @throws IOException
     */
    @Override
    public boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof LocalPathInfo);
        if (path.exists()) {
            if (path.isDirectory() && recursive) {
                FileUtils.deleteDirectory(((LocalPathInfo) path).file());
                return path.exists();
            } else {
                return ((LocalPathInfo) path).file().delete();
            }
        }
        return false;
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public List<String> list(@NonNull PathInfo path, boolean recursive) throws IOException {
        Preconditions.checkArgument(path instanceof LocalPathInfo);
        if (path.exists() && path.isDirectory()) {
            Collection<File> files = FileUtils.listFiles(((LocalPathInfo) path).file(), null, recursive);
            if (files != null && !files.isEmpty()) {
                List<String> paths = new ArrayList<>(files.size());
                for (File file : files) {
                    paths.add(file.getAbsolutePath());
                }
                return paths;
            }
        }
        return null;
    }

    @Override
    public List<String> find(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException {
        Preconditions.checkArgument(path instanceof LocalPathInfo);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileQuery));
        if (path.exists() && path.isDirectory()) {
            IOFileFilter dirFilter = null;
            if (!Strings.isNullOrEmpty(dirQuery)) {
                dirFilter = new LocalFileFilter(dirQuery);
            } else {
                dirFilter = TrueFileFilter.INSTANCE;
            }
            IOFileFilter fileFilter = new LocalFileFilter(fileQuery);
            Collection<File> files = FileUtils.listFiles(((LocalPathInfo) path).file(), fileFilter, dirFilter);
            if (files != null && !files.isEmpty()) {
                List<String> paths = new ArrayList<>(files.size());
                for (File file : files) {
                    paths.add(file.getAbsolutePath());
                }
                return paths;
            }
        }
        return null;
    }

    @Override
    public List<String> findFiles(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException {
        Preconditions.checkArgument(path instanceof LocalPathInfo);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileQuery));
        if (path.exists() && path.isDirectory()) {
            IOFileFilter dirFilter = null;
            dirFilter = TrueFileFilter.INSTANCE;

            IOFileFilter fileFilter = new LocalFileFilter(fileQuery).dirRegex(dirQuery);
            Collection<File> files = FileUtils.listFiles(((LocalPathInfo) path).file(), fileFilter, dirFilter);
            if (files != null && !files.isEmpty()) {
                List<String> paths = new ArrayList<>(files.size());
                for (File file : files) {
                    if (file.isFile())
                        paths.add(file.getAbsolutePath());
                }
                return paths;
            }
        }
        return null;
    }

    /**
     * @param path
     * @param createDir
     * @param overwrite
     * @return
     * @throws IOException
     */
    @Override
    public Writer writer(@NonNull PathInfo path, boolean createDir, boolean overwrite) throws IOException {
        if (!(path instanceof LocalPathInfo)) {
            throw new IOException(String.format("Invalid PathInfo instance. [passed=%s]",
                    path.getClass().getCanonicalName()));
        }
        if (createDir) {
            File dir = ((LocalPathInfo) path).file().getParentFile();
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    throw new IOException(String.format("Failed to create parent folder. [path=%s]", dir.getAbsolutePath()));
                }
            }
        }
        return new LocalWriter(path).open(overwrite);
    }

    /**
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Reader reader(@NonNull PathInfo path) throws IOException {
        if (!(path instanceof LocalPathInfo)) {
            throw new IOException(String.format("Invalid PathInfo instance. [passed=%s]",
                    path.getClass().getCanonicalName()));
        }
        if (!path.exists()) {
            throw new IOException(String.format("File not found. [path=%s]", ((LocalPathInfo) path).file().getAbsolutePath()));
        }
        return new LocalReader(path).open();
    }

    public static class LocalFileSystemConfig extends FileSystemConfig {
        public LocalFileSystemConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, path);
        }

        /**
         * @throws ConfigurationException
         */
        @Override
        public void read() throws ConfigurationException {
            super.read();
            if (Strings.isNullOrEmpty(rootPath())) {
                rootPath(System.getProperty("java.io.tmpdir"));
            }
        }
    }
}
