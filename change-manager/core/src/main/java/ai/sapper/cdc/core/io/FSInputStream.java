package ai.sapper.cdc.core.io;

import lombok.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FSInputStream extends InputStream {
    private final Reader reader;

    public FSInputStream(@NonNull Reader reader) {
        this.reader = reader;
    }

    @Override
    public int read() throws IOException {
        throw new IOException("Method not implemented...");
    }

    @Override
    public int read(byte[] b) throws IOException {
        return reader.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return reader.read(b, off, len);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        throw new IOException("Method not supported...");
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        byte[] buffer = new byte[len];
        reader.read(buffer);
        return buffer;
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return reader.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        throw new IOException("Method not supported...");
    }

    @Override
    public int available() throws IOException {
        throw new IOException("Method not supported...");
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new RuntimeException("Method not supported...");
    }

    @Override
    public synchronized void reset() throws IOException {
        reader.seek(0);
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        long bytes = 0;
        reader.seek(0);
        int bsize = 8096;
        byte[] buffer = new byte[bsize];
        while (true) {
            int r = read(buffer);
            if (r <= 0) break;
            out.write(buffer, 0, r);
            if (r < bsize) break;
        }
        out.flush();
        return bytes;
    }
}
