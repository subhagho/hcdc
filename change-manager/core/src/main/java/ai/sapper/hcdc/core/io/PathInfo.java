package ai.sapper.hcdc.core.io;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class PathInfo {
    private final String path;

    protected PathInfo(@NonNull String path) {
        this.path = path;
    }

    public String parent() {
        return FilenameUtils.getFullPath(path);
    }

    public String name() {
        return FilenameUtils.getName(path);
    }

    public String extension() {
        return FilenameUtils.getExtension(path);
    }

    public abstract boolean isDirectory() throws IOException;

    public abstract boolean isFile() throws IOException;

    public abstract boolean exists() throws IOException;
}
