package ai.sapper.hcdc.core.io;

import ai.sapper.hcdc.common.utils.PathUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class PathInfo {
    private final String path;
    private long dataSize = -1;

    protected PathInfo(@NonNull String path) {
        this.path = PathUtils.formatPath(path);
    }

    public String parent() {
        return FilenameUtils.getFullPath(path);
    }

    public abstract PathInfo parentPathInfo();

    public String name() {
        return FilenameUtils.getName(path);
    }

    public String extension() {
        return FilenameUtils.getExtension(path);
    }

    public abstract boolean isDirectory() throws IOException;

    public abstract boolean isFile() throws IOException;

    public abstract boolean exists() throws IOException;

    public abstract long size() throws IOException;
}
