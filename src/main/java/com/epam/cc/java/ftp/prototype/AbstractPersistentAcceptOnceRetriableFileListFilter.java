package com.epam.cc.java.ftp.prototype;

import org.springframework.integration.file.filters.FileListFilter;
import org.springframework.integration.file.filters.ResettableFileListFilter;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.util.Assert;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for filter with comprehensive strategy: retry after timeout, limit number of accepted files and etc.
 * <p>
 * Created by Maksym Bruner.
 */
public abstract class AbstractPersistentAcceptOnceRetriableFileListFilter<F>
        implements FileListFilter<F>, ResettableFileListFilter<F>, CommitableFilter<F>, Closeable {

    protected final ConcurrentMetadataStore store;

    protected final Flushable flushableStore;

    protected final String prefix;

    protected volatile boolean flushOnUpdate;

    protected int maxTry = 3;

    private long retryTimeoutSeconds = 60L;

    private int maxAcceptedFileListLength = -1;

    private final Object monitor = new Object();

    public AbstractPersistentAcceptOnceRetriableFileListFilter(ConcurrentMetadataStore store, String prefix) {
        Assert.notNull(store, "'store' cannot be null");
        Assert.notNull(prefix, "'prefix' cannot be null");
        this.store = store;
        this.prefix = prefix;
        if (store instanceof Flushable) {
            this.flushableStore = (Flushable) store;
        } else {
            this.flushableStore = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final List<F> filterFiles(F[] files) {
        List<F> accepted = new ArrayList<F>();
        if (files == null) {
            return accepted;
        }

        int acceptedCounter = 0;
        for (F file : files) {
            if (this.accept(file)) {
                accepted.add(file);
                acceptedCounter++;
                if (maxAcceptedFileListLength > 0 && acceptedCounter == maxAcceptedFileListLength) {
                    // limit number of accepted files if configured (maxAcceptedFileListLength greater than 0)
                    break;
                }
            }
        }

        return accepted;
    }

    protected boolean accept(F file) {
        String key = buildKey(file);
        synchronized (monitor) {
            long currentTimestamp = Instant.now().getEpochSecond();

            FileAcceptStatus status = new FileAcceptStatus();
            status.setLastTryTimestamp(currentTimestamp);
            status.setTries(1);

            String newValue = StatusSerializer.toString(status);
            String oldValue = store.putIfAbsent(key, newValue); // try happy path

            if (oldValue != null) {
                do {
                    oldValue = store.get(key);
                    status = StatusSerializer.fromString(oldValue);

                    if (status.getStatus() == FileAcceptStatus.DONE) {
                        return false;
                    } else if ((currentTimestamp - status.getLastTryTimestamp()) < retryTimeoutSeconds) {
                        return false;
                    }

                    if (status.getTries() >= maxTry) {
                        status.setStatus(FileAcceptStatus.REJECTED);
                    } else {
                        status.setLastTryTimestamp(currentTimestamp);
                        status.setTries(status.getTries() + 1);
                    }

                    newValue = StatusSerializer.toString(status);
                } while (!store.replace(key, oldValue, newValue));
            }

            return status.getStatus() == FileAcceptStatus.IN_PROGRESS;
        }
    }

    @Override
    public void close() throws IOException {
        if (this.store instanceof Closeable) {
            ((Closeable) this.store).close();
        }
    }

    @Override
    public boolean remove(F f) {
        String removed = this.store.remove(buildKey(f));
        flushIfNeeded();
        return removed != null;
    }

    @Override
    public void commit(F file) {
        String key = buildKey(file);
        synchronized (monitor) {
            String oldValue;
            String newValue;

            do {
                oldValue = store.get(key);
                FileAcceptStatus status;

                if (oldValue != null) {
                    status = StatusSerializer.fromString(oldValue);

                    if (status.getStatus() == FileAcceptStatus.DONE) {
                        /*
                         * another process finished processing before our process - this should be reported
                         * with high severity and timeout value must be increased
                         */
                        return;
                    }
                } else {
                    // very strange situation when file was processed without creating record in metadata store
                    status = new FileAcceptStatus();
                }

                status.setStatus(FileAcceptStatus.DONE);
                newValue = StatusSerializer.toString(status);
            } while (!store.replace(key, oldValue, newValue));
        }
    }

    /**
     * Determine whether the metadataStore should be flushed on each update (if {@link Flushable}).
     *
     * @param flushOnUpdate true to flush.
     * @since 4.1.5
     */
    public void setFlushOnUpdate(boolean flushOnUpdate) {
        this.flushOnUpdate = flushOnUpdate;
    }

    /**
     * Flush the store if it's a {@link Flushable} and
     * {@link #setFlushOnUpdate(boolean) flushOnUpdate} is true.
     *
     * @since 1.4.5
     */
    protected void flushIfNeeded() {
        if (this.flushOnUpdate && this.flushableStore != null) {
            try {
                this.flushableStore.flush();
            } catch (IOException e) {
                // store's responsibility to log
            }
        }
    }

    /**
     * The default key is the {@link #prefix} plus the full filename.
     *
     * @param file The file.
     * @return The key.
     */
    protected String buildKey(F file) {
        return this.prefix + this.fileName(file);
    }

    protected abstract String fileName(F file);

    public int getMaxTry() {
        return maxTry;
    }

    public void setMaxTry(int maxTry) {
        this.maxTry = maxTry;
    }

    public long getRetryTimeoutSeconds() {
        return retryTimeoutSeconds;
    }

    public void setRetryTimeoutSeconds(long retryTimeoutSeconds) {
        this.retryTimeoutSeconds = retryTimeoutSeconds;
    }

    public int getMaxAcceptedFileListLength() {
        return maxAcceptedFileListLength;
    }

    public void setMaxAcceptedFileListLength(int maxAcceptedFileListLength) {
        this.maxAcceptedFileListLength = maxAcceptedFileListLength;
    }
}
