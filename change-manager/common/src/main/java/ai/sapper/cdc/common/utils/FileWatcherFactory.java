package ai.sapper.cdc.common.utils;

import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileWatcherFactory {
    private static final Map<String, FileWatcher> watchers = new HashMap<>();
    private static final Map<String, Thread> runners = new HashMap<>();

    public static FileWatcher create(@NonNull String name, @NonNull String directory, String regex, @NonNull FileWatcher.FileWatcherCallback callback) throws IOException {
        synchronized (watchers) {
            if (watchers.containsKey(name)) {
                FileWatcher watcher = watchers.get(name);
                Preconditions.checkNotNull(watcher);
                return watcher;
            }
            FileWatcher watcher = new FileWatcher(directory, regex).withCallback(callback);
            Thread runner = new Thread(watcher);
            runners.put(name, runner);
            watchers.put(name, watcher);

            runner.start();
            DefaultLogger.LOGGER.debug(String.format("Created new file watcher. [name=%s, path=%s, regex=%s]",
                    name, watcher.directory(), watcher.regex()));
            return watcher;
        }
    }

    public static FileWatcher get(String name) {
        if (watchers.containsKey(name)) return watchers.get(name);
        return null;
    }

    public static boolean stop(String name) throws InterruptedException {
        synchronized (watchers) {
            FileWatcher watcher = get(name);
            if (watcher != null) {
                watcher.stop();
                Thread runner = runners.get(name);
                Preconditions.checkNotNull(runner);
                runner.join();
                watchers.remove(name);
                runners.remove(name);
                return true;
            }
            return false;
        }
    }

    public static void shutdown() {
        synchronized (watchers) {
            for (String name : watchers.keySet()) {
                try {
                    if (!stop(name)) {
                        DefaultLogger.LOGGER.warn(String.format("Failed to stop watcher. [name=%s]", name));
                    } else {
                        DefaultLogger.LOGGER.debug(String.format("Stopped watcher. [name=%s]", name));
                    }
                } catch (InterruptedException ie) {
                    DefaultLogger.LOGGER.error(String.format("Error stopping watcher. [name=%s]", name), ie);
                }
            }
        }
    }
}
