package ai.sapper.cdc.core.io;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class Writer implements Closeable {
    private final PathInfo path;

    protected Writer(@NonNull PathInfo path) {
        this.path = path;
    }

    public abstract Writer open(boolean overwrite) throws IOException;

    public Writer open() throws IOException {
        return open(false);
    }

    public abstract long write(byte[] data, long offset, long length) throws IOException;

    public long write(byte[] data) throws IOException {
        return write(data, 0, data.length);
    }

    public abstract void flush() throws IOException;

    public abstract long truncate(long offset, long length) throws IOException;

    public long truncate(int length) throws IOException {
        return truncate(0, length);
    }

    public abstract boolean isOpen();
}
