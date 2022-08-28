package ai.sapper.cdc.common.utils;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class FileWatcher implements Runnable {
    private static final long __POLL_TIMEOUT = 100;

    private final String directory;
    private final String regex;
    private final Pattern pattern;

    private FileWatcherCallback callback;

    private boolean running = false;

    public FileWatcher(@NonNull String directory, @NonNull String regex) {
        File di = new File(directory);
        Preconditions.checkState(di.exists());

        this.directory = directory;
        this.regex = regex;
        this.pattern = Pattern.compile(regex);
    }

    public FileWatcher withCallback(@NonNull FileWatcherCallback callback) {
        this.callback = callback;
        return this;
    }

    public void run() {
        Preconditions.checkState(callback != null);
        running = true;
        final Path path = Paths.get(directory);
        DefaultLogger.LOGGER.info(String.format("Starting file watcher. [directory=%s][regex=%s]", directory, regex));
        try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
            final WatchKey watchKey = path.register(watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_CREATE);

            while (running) {
                final WatchKey wk = watchService.poll(__POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                if (wk != null) {
                    for (WatchEvent<?> event : wk.pollEvents()) {
                        //we only register "ENTRY_MODIFY" so the context is always a Path.
                        final Path changed = (Path) event.context();
                        final String name = changed.toFile().getName();
                        final Matcher matcher = pattern.matcher(name);
                        if (matcher.matches()) {
                            Path cp = Paths.get(String.format("%s/%s", directory, changed.toString()));
                            callback.handle(cp.toAbsolutePath().toString(), event.kind());
                        }
                    }
                    // reset the key
                    boolean valid = wk.reset();
                    if (!valid) {
                        throw new IOException(String.format("[%s] Key no longer valid.", path.toAbsolutePath().toString()));
                    }
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
        void handle(@NonNull String path, WatchEvent.Kind<?> eventKind) throws IOException;
    }
}
