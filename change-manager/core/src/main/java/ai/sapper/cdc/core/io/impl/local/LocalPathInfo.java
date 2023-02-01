package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.core.io.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class LocalPathInfo extends PathInfo {
    private File file;

    protected LocalPathInfo(@NonNull String path, @NonNull String domain) {
        super(path, domain);
        file = new File(path);
    }

    public LocalPathInfo(@NonNull Map<String, String> config) {
        super(config);
        file = new File(path());
    }

    protected LocalPathInfo(@NonNull File file, @NonNull String domain) {
        super(file.getAbsolutePath(), domain);
        this.file = file;
    }

    /**
     * @return
     */
    @Override
    public PathInfo parentPathInfo() throws Exception {
        return new LocalPathInfo(file.getParentFile().getAbsolutePath(),
                domain());
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

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return super.toString();
    }
}
