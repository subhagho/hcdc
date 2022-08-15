package ai.sapper.hcdc.core.io;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class Reader implements Closeable {
    private final PathInfo path;

    protected Reader(@NonNull PathInfo path) {
        this.path = path;
    }

    public abstract Reader open() throws IOException;

    public abstract int read(byte[] buffer, int offset, int length) throws IOException;

    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    public abstract void seek(int offset) throws IOException;

    public abstract boolean isOpen();
}
