package ai.sapper.hcdc.common.utils;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

@Getter
@Accessors(fluent = true)
public class FileWatcher implements Runnable {
    private final String directory;
    private final String filename;

    private FileWatcherCallback callback;

    private boolean running = false;

    public FileWatcher(@NonNull String directory, @NonNull String filename) {
        File di = new File(directory);
        Preconditions.checkState(di.exists());
        String path = String.format("%s/%s", di.getAbsolutePath(), filename);
        File fi = new File(path);
        Preconditions.checkState(fi.exists());
        this.directory = directory;
        this.filename = filename;
    }

    public FileWatcher(@NonNull String file) {
        File fi = new File(file);

        directory = fi.getParent();
        filename = fi.getName();
    }

    public FileWatcher withCallback(@NonNull FileWatcherCallback callback) {
        this.callback = callback;
        return this;
    }

    public Path path() {
        return FileSystems.getDefault().getPath(String.format("%s/%s", directory, filename));
    }

    public void run() {
        Preconditions.checkState(callback != null);
        running = true;
        final Path path = FileSystems.getDefault().getPath(directory);
        try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
            final WatchKey watchKey = path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

            while (running) {
                final WatchKey wk = watchService.take();
                for (WatchEvent<?> event : wk.pollEvents()) {
                    //we only register "ENTRY_MODIFY" so the context is always a Path.
                    final Path changed = (Path) event.context();
                    if (changed.endsWith(filename)) {
                        callback.handle(changed.toAbsolutePath().toString());
                    }
                }
                // reset the key
                boolean valid = wk.reset();
                if (!valid) {
                    throw new IOException(String.format("[%s] Key no longer valid.", path.toAbsolutePath().toString()));
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        running = false;
    }

    public static interface FileWatcherCallback {
        void handle(@NonNull String path) throws IOException;
    }
}
