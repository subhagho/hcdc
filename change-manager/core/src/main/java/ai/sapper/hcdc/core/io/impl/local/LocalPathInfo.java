package ai.sapper.hcdc.core.io.impl.local;

import ai.sapper.hcdc.core.io.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
     */
    @Override
    public PathInfo parentPathInfo() {
        return new LocalPathInfo(file.getParentFile().getAbsolutePath());
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

    /**
     * @return
     * @throws IOException
     */
    @Override
    public long size() throws IOException {
        if (dataSize() < 0) {
            if (!exists()) {
                dataSize(0);
            } else {
                Path p = Paths.get(file.toURI());
                dataSize(Files.size(p));
            }
        }
        return dataSize();
    }
}
