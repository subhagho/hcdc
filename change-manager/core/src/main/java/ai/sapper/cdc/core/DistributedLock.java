package ai.sapper.cdc.core;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedActionException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@Accessors(fluent = true)
public class DistributedLock extends ReentrantLock implements Closeable {
    private static final int DEFAULT_LOCK_TIMEOUT = 15000;

    private final LockId id;
    private long lockTimeout = DEFAULT_LOCK_TIMEOUT;

    @Getter(AccessLevel.NONE)
    private InterProcessMutex mutex = null;
    private ZookeeperConnection connection;
    private final String zkBasePath;

    public DistributedLock(@NonNull LockId id, @NonNull String zkBasePath) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zkBasePath));
        this.id = id;
        this.zkBasePath = zkBasePath;
    }

    public DistributedLock(@NonNull String namespace, @NonNull String name, @NonNull String zkBasePath) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zkBasePath));
        this.zkBasePath = zkBasePath;
        id = new LockId(namespace, name);
    }

    public DistributedLock withConnection(@NonNull ZookeeperConnection connection) {
        this.connection = connection;
        mutex = new InterProcessMutex(connection.client(), lockPath());

        return this;
    }

    public DistributedLock withLockTimeout(long lockTimeout) {
        Preconditions.checkArgument(lockTimeout > 0);
        this.lockTimeout = lockTimeout;

        return this;
    }

    private String lockPath() {
        return new PathUtils.ZkPathBuilder()
                .withPath(zkBasePath)
                .withPath(id.namespace)
                .withPath("__locks")
                .withPath(id.name)
                .build();
    }

    /**
     * Acquires the lock.
     *
     * <p>Acquires the lock if it is not held by another thread and returns
     * immediately, setting the lock hold count to one.
     *
     * <p>If the current thread already holds the lock then the hold
     * count is incremented by one and the method returns immediately.
     *
     * <p>If the lock is held by another thread then the
     * current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired,
     * at which time the lock hold count is set to one.
     */
    @Override
    public void lock() {
        Preconditions.checkState(mutex != null);
        try {
            if (!tryLock()) {
                throw new LockError(String.format("[%s][%s] Timeout getting lock.", id.namespace, id.name));
            }
        } catch (Throwable ex) {
            throw new LockError(ex);
        }
    }

    /**
     * Acquires the lock only if it is not held by another thread at the time
     * of invocation.
     *
     * <p>Acquires the lock if it is not held by another thread and
     * returns immediately with the value {@code true}, setting the
     * lock hold count to one. Even when this lock has been set to use a
     * fair ordering policy, a call to {@code tryLock()} <em>will</em>
     * immediately acquire the lock if it is available, whether or not
     * other threads are currently waiting for the lock.
     * This &quot;barging&quot; behavior can be useful in certain
     * circumstances, even though it breaks fairness. If you want to honor
     * the fairness setting for this lock, then use
     * {@link #tryLock(long, TimeUnit) tryLock(0, TimeUnit.SECONDS)}
     * which is almost equivalent (it also detects interruption).
     *
     * <p>If the current thread already holds this lock then the hold
     * count is incremented by one and the method returns {@code true}.
     *
     * <p>If the lock is held by another thread then this method will return
     * immediately with the value {@code false}.
     *
     * @return {@code true} if the lock was free and was acquired by the
     * current thread, or the lock was already held by the current
     * thread; and {@code false} otherwise
     */
    @Override
    public boolean tryLock() {
        try {
            return tryLock(lockTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            DefaultLogger.LOGGER.debug(String.format("Lock Timeout: [name=%s]", id.toString()));
            return false;
        }
    }

    /**
     * Acquires the lock if it is not held by another thread within the given
     * waiting time and the current thread has not been
     * {@linkplain Thread#interrupt interrupted}.
     *
     * <p>Acquires the lock if it is not held by another thread and returns
     * immediately with the value {@code true}, setting the lock hold count
     * to one. If this lock has been set to use a fair ordering policy then
     * an available lock <em>will not</em> be acquired if any other threads
     * are waiting for the lock. This is in contrast to the {@link #tryLock()}
     * method. If you want a timed {@code tryLock} that does permit barging on
     * a fair lock then combine the timed and un-timed forms together:
     *
     * <pre> {@code
     * if (lock.tryLock() ||
     *     lock.tryLock(timeout, unit)) {
     *   ...
     * }}</pre>
     *
     * <p>If the current thread
     * already holds this lock then the hold count is incremented by one and
     * the method returns {@code true}.
     *
     * <p>If the lock is held by another thread then the
     * current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of three things happens:
     *
     * <ul>
     *
     * <li>The lock is acquired by the current thread; or
     *
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     *
     * <li>The specified waiting time elapses
     *
     * </ul>
     *
     * <p>If the lock is acquired then the value {@code true} is returned and
     * the lock hold count is set to one.
     *
     * <p>If the current thread:
     *
     * <ul>
     *
     * <li>has its interrupted status set on entry to this method; or
     *
     * <li>is {@linkplain Thread#interrupt interrupted} while
     * acquiring the lock,
     *
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     * <p>In this implementation, as this method is an explicit
     * interruption point, preference is given to responding to the
     * interrupt over normal or reentrant acquisition of the lock, and
     * over reporting the elapse of the waiting time.
     *
     * @param timeout the time to wait for the lock
     * @param unit    the time unit of the timeout argument
     * @return {@code true} if the lock was free and was acquired by the
     * current thread, or the lock was already held by the current
     * thread; and {@code false} if the waiting time elapsed before
     * the lock could be acquired
     * @throws InterruptedException if the current thread is interrupted
     * @throws NullPointerException if the time unit is null
     */
    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        Preconditions.checkState(mutex != null);
        if (super.tryLock(timeout, unit)) {
            if (!mutex.isAcquiredInThisProcess()) {
                try {
                    return mutex.acquire(timeout, unit);
                } catch (Throwable t) {
                    super.unlock();
                    throw new LockError(t);
                }
            }
            return true;
        }
        return false;
    }

    private boolean isLockByThread() {
        return (isHeldByCurrentThread() && mutex.isAcquiredInThisProcess());
    }

    /**
     * Attempts to release this lock.
     *
     * <p>If the current thread is the holder of this lock then the hold
     * count is decremented.  If the hold count is now zero then the lock
     * is released.  If the current thread is not the holder of this
     * lock then {@link IllegalMonitorStateException} is thrown.
     *
     * @throws IllegalMonitorStateException if the current thread does not
     *                                      hold this lock
     */
    @Override
    public void unlock() {
        Preconditions.checkState(mutex != null);
        try {
            if (mutex.isAcquiredInThisProcess()) {
                if (getHoldCount() == 1)
                    mutex.release();
            } else {
                throw new LockError(String.format("[%s][%s] Lock not held by current thread.", id.namespace, id.name));
            }
            super.unlock();
        } catch (Throwable t) {
            throw new LockError(t);
        }
    }

    /**
     * Queries if this lock is held by any thread. This method is
     * designed for use in monitoring of the system state,
     * not for synchronization control.
     *
     * @return {@code true} if any thread holds this lock and
     * {@code false} otherwise
     */
    @Override
    public boolean isLocked() {
        Preconditions.checkState(mutex != null);
        if (super.isLocked()) {
            return mutex.isAcquiredInThisProcess();
        }
        return false;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (getHoldCount() == 0) {
            mutex = null;
        }
    }

    public static final class LockError extends RuntimeException {
        private static final String __PREFIX = "Distributed Lock Error";

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param message the detail message. The detail message is saved for
         *                later retrieval by the {@link #getMessage()} method.
         */
        public LockError(String message) {
            super(String.format("%s: %s", __PREFIX, message));
        }

        /**
         * Constructs a new exception with the specified detail message and
         * cause.  <p>Note that the detail message associated with
         * {@code cause} is <i>not</i> automatically incorporated in
         * this exception's detail message.
         *
         * @param message the detail message (which is saved for later retrieval
         *                by the {@link #getMessage()} method).
         * @param cause   the cause (which is saved for later retrieval by the
         *                {@link #getCause()} method).  (A {@code null} value is
         *                permitted, and indicates that the cause is nonexistent or
         *                unknown.)
         * @since 1.4
         */
        public LockError(String message, Throwable cause) {
            super(String.format("%s: %s", __PREFIX, message), cause);
        }

        /**
         * Constructs a new exception with the specified cause and a detail
         * message of {@code (cause==null ? null : cause.toString())} (which
         * typically contains the class and detail message of {@code cause}).
         * This constructor is useful for exceptions that are little more than
         * wrappers for other throwables (for example, {@link
         * PrivilegedActionException}).
         *
         * @param cause the cause (which is saved for later retrieval by the
         *              {@link #getCause()} method).  (A {@code null} value is
         *              permitted, and indicates that the cause is nonexistent or
         *              unknown.)
         * @since 1.4
         */
        public LockError(Throwable cause) {
            super(String.format("%s: %s", __PREFIX, cause.getLocalizedMessage()), cause);
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class LockId implements Serializable {
        private String namespace;
        private String name;

        public LockId() {
        }

        public LockId(@NonNull String namespace, @NonNull String name) {
            this.namespace = namespace;
            this.name = name;
        }

        public String lockPath(String pathPrefix) {
            if (Strings.isNullOrEmpty(pathPrefix)) {
                return String.format("%s/%s", namespace, name);
            } else {
                return String.format("%s/%s/%s", pathPrefix, namespace, name);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LockId)) return false;
            LockId lockId = (LockId) o;
            return namespace.equals(lockId.namespace) && name.equals(lockId.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, name);
        }

        @Override
        public String toString() {
            return "LockId{" +
                    "namespace='" + namespace + '\'' +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static boolean withRetry(@NonNull DistributedLock lock,
                                    int retryCount,
                                    long sleepInterval) throws Exception {
        int count = 0;
        while (true) {
            try {
                lock.lock();
                return true;
            } catch (DistributedLock.LockError le) {
                if (count > retryCount) {
                    DefaultLogger.LOGGER.error(
                            String.format("Error acquiring lock. [error=%s][retries=%d]",
                                    le.getLocalizedMessage(), retryCount));
                    return false;
                }
                DefaultLogger.LOGGER.warn(String.format("Failed to acquire lock, will retry... [error=%s][retries=%d]",
                        le.getLocalizedMessage(), retryCount));
                Thread.sleep(sleepInterval);
                count++;
            }
        }
    }
}
