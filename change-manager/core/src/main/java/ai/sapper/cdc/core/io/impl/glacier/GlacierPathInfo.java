package ai.sapper.cdc.core.io.impl.glacier;

import ai.sapper.cdc.core.io.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;
import software.amazon.awssdk.services.glacier.GlacierClient;

import java.io.IOException;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class GlacierPathInfo extends PathInfo {
    public static final String CONFIG_KEY_VAULT = "vault";
    private final GlacierClient client;
    private final String vault;


    protected GlacierPathInfo(@NonNull GlacierClient client,
                              @NonNull String path,
                              @NonNull String domain,
                              @NonNull String vault) {
        super(path, domain);
        this.client = client;
        this.vault = vault;
        archive(true);
    }

    protected GlacierPathInfo(@NonNull GlacierClient client,
                              @NonNull Map<String, String> config) {
        super(config);
        this.client = client;
        vault = config.get(CONFIG_KEY_VAULT);
        archive(true);
    }

    @Override
    public PathInfo parentPathInfo() {
        String path = path();
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 2);
        }
        String p = FilenameUtils.getFullPath(path);
        return new GlacierPathInfo(client, path, domain(), vault);
    }

    @Override
    public boolean isDirectory() throws IOException {
        return path().endsWith("/");
    }

    @Override
    public boolean isFile() throws IOException {
        return exists();
    }

    @Override
    public boolean exists() throws IOException {
        return false;
    }

    @Override
    public long size() throws IOException {
        return 0;
    }

    @Override
    public Map<String, String> pathConfig() {
        Map<String, String> config =  super.pathConfig();
        config.put(CONFIG_KEY_VAULT, vault);
        return config;
    }
}
