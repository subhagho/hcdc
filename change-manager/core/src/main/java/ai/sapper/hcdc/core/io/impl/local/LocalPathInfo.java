package ai.sapper.hcdc.core.io.impl.local;

import ai.sapper.hcdc.core.io.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class LocalPathInfo extends PathInfo {
    private final File file;

    protected LocalPathInfo(@NonNull String path) {
        super(path);
        file = new File(path);
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean isDirectory() throws IOException {
        return file.isDirectory();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean isFile() throws IOException {
        return file.isFile();
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean exists() throws IOException {
        return file.exists();
    }
}
