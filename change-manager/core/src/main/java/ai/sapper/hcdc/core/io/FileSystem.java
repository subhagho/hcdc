package ai.sapper.hcdc.core.io;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class FileSystem {
    private PathInfo root;

    public abstract FileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config, String pathPrefix) throws IOException;

    public abstract PathInfo get(@NonNull String path, String... options) throws IOException;

    public abstract String mkdir(@NonNull PathInfo path, @NonNull String name) throws IOException;

    public abstract String mkdirs(@NonNull PathInfo path) throws IOException;

    public abstract boolean delete(@NonNull PathInfo path, boolean recursive) throws IOException;

    public boolean delete(@NonNull PathInfo path) throws IOException {
        return delete(path, false);
    }

    public abstract List<String> list(@NonNull PathInfo path, boolean recursive) throws IOException;

    public abstract List<String> find(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException;

    public abstract List<String> findFiles(@NonNull PathInfo path, String dirQuery, @NonNull String fileQuery) throws IOException;

    public boolean exists(@NonNull String path) throws IOException {
        PathInfo pi = get(path);
        if (pi != null) return pi.exists();
        return false;
    }

    public boolean isDirectory(@NonNull String path) throws IOException {
        PathInfo pi = get(path);
        if (pi != null) return pi.isDirectory();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public boolean isFile(@NonNull String path) throws IOException {
        PathInfo pi = get(path);
        if (pi != null) return pi.isFile();
        else {
            throw new IOException(String.format("File not found. [path=%s]", path));
        }
    }

    public abstract Writer writer(@NonNull PathInfo path, boolean createDir, boolean overwrite) throws IOException;

    public Writer writer(@NonNull PathInfo path, boolean overwrite) throws IOException {
        return writer(path, false, overwrite);
    }

    public Writer writer(@NonNull PathInfo path) throws IOException {
        return writer(path, false, false);
    }

    public abstract Reader reader(@NonNull PathInfo path) throws IOException;

}
