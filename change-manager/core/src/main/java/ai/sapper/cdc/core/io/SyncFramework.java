package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.AbstractState;
import ai.sapper.cdc.common.ConfigReader;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Getter
@Accessors(fluent = true)
public abstract class SyncFramework implements Closeable {

    private ExecutorService executor;
    private SyncFrameworkConfig config;

    public SyncFramework init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        config = new SyncFrameworkConfig(xmlConfig);
        config.read();
        executor = new ThreadPoolExecutor(config.poolSize,
                config.poolSize,
                config.timeout,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(config.queueSize));

        return this;
    }

    public void schedule(@NonNull Task<?> task) throws Exception {
        if (executor == null) {
            throw new Exception("Executor not initialised...");
        }
        if (executor.isShutdown()) {
            throw new Exception("Executor has been terminated...");
        }
        executor.submit(task);
    }

    public abstract void download(@NonNull PathInfo path) throws Exception;

    public abstract void upload(@NonNull PathInfo path) throws Exception;

    @Override
    public void close() throws IOException {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    @Getter
    @Accessors(fluent = true)
    public static class SyncFrameworkConfig extends ConfigReader {
        public static final String __CONFIG_PATH = "sync";

        public static final class Constants {
            public static final String CONFIG_POOL_SIZE = "poolSize";
            public static final String CONFIG_TIMEOUT = "timeout";
            public static final String CONFIG_QUEUE_SIZE = "queueSize";
        }

        private int poolSize = 4;
        private long timeout = 500;
        private int queueSize = 32;

        public SyncFrameworkConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config) {
            super(config, __CONFIG_PATH);
        }

        public void read() throws ConfigurationException {
            if (get().containsKey(Constants.CONFIG_POOL_SIZE)) {
                poolSize = get().getInt(Constants.CONFIG_POOL_SIZE);
            }
            if (get().containsKey(Constants.CONFIG_TIMEOUT)) {
                timeout = get().getLong(Constants.CONFIG_TIMEOUT);
            }
            if (get().containsKey(Constants.CONFIG_QUEUE_SIZE)) {
                queueSize = get().getInt(Constants.CONFIG_QUEUE_SIZE);
            }
        }
    }

    public interface SyncCallback<T> {
        void onSuccess(@NonNull PathInfo path, T response);

        void onError(@NonNull PathInfo pathInfo, Throwable error);
    }

    public enum ETaskState {
        Pending, Running, Success, Error
    }

    public static class TaskState extends AbstractState<ETaskState> {

        public TaskState() {
            super(ETaskState.Error);
            state(ETaskState.Pending);
        }
    }

    @Getter
    @Accessors(fluent = true)
    public abstract static class Task<T> implements Runnable {
        private final PathInfo path;
        private final long timestamp;
        private long completedTime;
        private SyncCallback<T> callback;
        private final TaskState state = new TaskState();
        private T response = null;

        public Task(@NonNull PathInfo path) {
            this.path = path;
            timestamp = System.currentTimeMillis();
        }

        @Override
        public void run() {
            state.state(ETaskState.Running);
            try {
                response = execute();
                completed();
            } catch (Throwable t) {
                error(t);
            }
        }

        public abstract T execute() throws Exception;

        public Task<T> withCallback(@NonNull SyncCallback<T> callback) {
            this.callback = callback;
            return this;
        }

        public Task<T> completed() {
            state.state(ETaskState.Success);
            completedTime = System.currentTimeMillis();
            if (callback != null) {
                callback.onSuccess(path, response);
            }
            return this;
        }

        public Task<T> error(@NonNull Throwable error) {
            state.error(error);
            completedTime = System.currentTimeMillis();
            if (callback != null) {
                callback.onError(path, error);
            }
            return this;
        }
    }

    public static abstract class DownloadTask<T> extends Task<T> {

        public DownloadTask(@NonNull PathInfo path) {
            super(path);
        }
    }

    public static abstract class UploadTask<T> extends Task<T> {

        public UploadTask(@NonNull PathInfo path) {
            super(path);
        }
    }
}
