package ai.sapper.hcdc.core.io;

import ai.sapper.hcdc.common.utils.PathUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class PathInfo {
    public static final String CONFIG_KEY_DOMAIN = "domain";
    public static final String CONFIG_KEY_PATH = "path";

    private final String domain;
    private final String path;
    private long dataSize = -1;

    protected PathInfo(@NonNull String path, @NonNull String domain) {
        this.path = PathUtils.formatPath(path);
        this.domain = domain;
    }

    protected PathInfo(@NonNull Map<String, String> config) {
        domain = config.get(CONFIG_KEY_DOMAIN);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
        path = config.get(CONFIG_KEY_PATH);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
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

    public Map<String, String> pathConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(CONFIG_KEY_DOMAIN, domain);
        config.put(CONFIG_KEY_PATH, path);

        return config;
    }
}
