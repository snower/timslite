package io.github.snower.timslite;

import io.github.snower.timslite.errors.TmslException;
import io.github.snower.timslite.uniffi.CreateDatasetOptions;
import io.github.snower.timslite.uniffi.JournalQueueBridge;
import io.github.snower.timslite.uniffi.QueueBridge;
import io.github.snower.timslite.uniffi.StoreConfig;
import java.util.ArrayList;
import java.util.List;

/**
 * Main entry point for the timslite storage engine.
 *
 * <p>A {@code Store} manages datasets, queues, journal access, and background tasks.
 * It wraps the native Rust storage engine through UniFFI-generated Kotlin/JVM bindings.
 * All resources are released when the store is closed.</p>
 *
 * <p>This class implements {@link AutoCloseable} and should be used with
 * try-with-resources to guarantee cleanup:</p>
 *
 * <pre>{@code
 * StoreConfig config = StoreConfigBuilder.builder()
 *         .enableJournal(true)
 *         .build();
 * try (Store store = Store.open("/path/to/data", config)) {
 *     store.createDataset("metrics", "cpu", CreateDatasetOptionsBuilder.builder().build());
 *     try (Dataset ds = store.openDataset("metrics", "cpu")) {
 *         ds.write(1700000000L, new byte[]{1, 2, 3});
 *         Record rec = ds.read(1700000000L);
 *     }
 * }
 * }</pre>
 *
 * <p>Timestamps throughout this API are signed 64-bit values ({@code long}).
 * Data payloads are raw {@code byte[]} arrays. Individual dataset operations
 * are not thread-safe; external synchronization is required when sharing a
 * {@code Dataset} across threads.</p>
 */
public final class Store implements AutoCloseable {
    private final io.github.snower.timslite.uniffi.StoreBridge bridge;
    private boolean closed;

    private Store(io.github.snower.timslite.uniffi.StoreBridge bridge) {
        this.bridge = bridge;
        this.closed = false;
    }

    /**
     * Opens a store at the given path with default configuration.
     *
     * @param path filesystem directory for the store
     * @return the opened store
     * @throws TmslException if the store cannot be opened
     */
    public static Store open(String path) {
        try {
            io.github.snower.timslite.uniffi.StoreBridge bridge =
                io.github.snower.timslite.uniffi.StoreBridge.Companion.open(
                    path,
                    io.github.snower.timslite.StoreConfigBuilder.builder().build());
            return new Store(bridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Opens a store at the given path with the specified configuration.
     *
     * @param path   filesystem directory for the store
     * @param config store configuration
     * @return the opened store
     * @throws TmslException if the store cannot be opened
     */
    public static Store open(String path, io.github.snower.timslite.uniffi.StoreConfig config) {
        try {
            io.github.snower.timslite.uniffi.StoreBridge bridge =
                io.github.snower.timslite.uniffi.StoreBridge.Companion.open(path, config);
            return new Store(bridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /** Closes this store and releases all native resources. */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            bridge.close();
        }
    }

    /**
     * Returns whether this store has been closed.
     *
     * @return {@code true} if {@link #close()} has been called
     */
    public boolean isClosed() {
        return closed;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Store is closed");
        }
    }

    /**
     * Returns whether this store was opened in read-only mode.
     *
     * @return {@code true} if the store is read-only
     * @throws TmslException if the query fails
     */
    public boolean isReadOnly() {
        checkNotClosed();
        try {
            return bridge.isReadOnly();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Creates a new dataset with the given name and type.
     *
     * @param name    dataset name, must match {@code ^[0-9A-Za-z_-]+$}
     * @param type    dataset type, must match {@code ^[0-9A-Za-z_-]+$}
     * @param options creation options (may carry a custom {@code DatasetConfig})
     * @throws TmslException if creation fails or the dataset already exists
     */
    public void createDataset(String name, String type, CreateDatasetOptions options) {
        checkNotClosed();
        try {
            bridge.createDataset(name, type, options);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Opens an existing dataset by name and type.
     *
     * @param name dataset name
     * @param type dataset type
     * @return the opened dataset
     * @throws TmslException if the dataset does not exist or cannot be opened
     */
    public Dataset openDataset(String name, String type) {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.DatasetBridge datasetBridge = bridge.openDataset(name, type);
            return new Dataset(datasetBridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Opens an existing dataset by its numeric identifier.
     *
     * @param identifier the dataset identifier returned by {@link InspectResult}
     * @return the opened dataset
     * @throws TmslException if no dataset with that identifier exists
     */
    public Dataset openDatasetByIdentifier(long identifier) {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.DatasetBridge datasetBridge =
                KotlinConversions.callOpenDatasetByIdentifier(bridge, identifier);
            return new Dataset(datasetBridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Drops a dataset and removes its on-disk files.
     *
     * @param name dataset name
     * @param type dataset type
     * @throws TmslException if the dataset does not exist or cannot be dropped
     */
    public void dropDataset(String name, String type) {
        checkNotClosed();
        try {
            bridge.dropDataset(name, type);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Returns the names of all datasets in this store.
     *
     * @return list of dataset names
     * @throws TmslException if the query fails
     */
    public List<String> getDatasetNames() {
        checkNotClosed();
        try {
            return bridge.getDatasetNames();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Returns the types registered for a given dataset name.
     *
     * @param name dataset name
     * @return list of type strings
     * @throws TmslException if the query fails
     */
    public List<String> getDatasetTypes(String name) {
        checkNotClosed();
        try {
            return bridge.getDatasetTypes(name);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Inspects a dataset, returning its configuration and runtime state.
     *
     * @param name dataset name
     * @param type dataset type
     * @return inspection result containing info and state
     * @throws TmslException if the dataset does not exist
     */
    public InspectResult inspectDataset(String name, String type) {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.DataSetInspectResult kotlinResult =
                bridge.inspectDataset(name, type);
            return new InspectResult(kotlinResult);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Runs pending background tasks (flush, idle-close, cache eviction, retention reclaim).
     *
     * @return result describing what was executed and the next recommended delay
     * @throws TmslException if a background task fails
     */
    public TickResult tickBackgroundTasks() {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.TickResult kotlinResult = bridge.tickBackgroundTasks();
            return new TickResult(kotlinResult);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Returns the recommended delay in milliseconds before the next background tick.
     *
     * @return delay in milliseconds
     */
    public long nextBackgroundDelayMs() {
        checkNotClosed();
        return KotlinConversions.getULong(bridge, "nextBackgroundDelayMs");
    }

    // ---- Queue API ----

    /**
     * Opens a persistent queue on the given dataset.
     *
     * @param dataset the dataset to use as queue backing
     * @return the queue handle
     * @throws TmslException if the queue cannot be opened
     */
    public Queue openQueue(Dataset dataset) {
        checkNotClosed();
        try {
            QueueBridge queueBridge = bridge.openQueue(dataset.bridge());
            return new Queue(queueBridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    // ---- Journal API ----

    /**
     * Opens the journal queue for consuming change-log records.
     *
     * @return the journal queue handle
     * @throws TmslException if journal is not enabled or cannot be opened
     */
    public JournalQueue openJournalQueue() {
        checkNotClosed();
        try {
            JournalQueueBridge jqBridge = bridge.openJournalQueue();
            return new JournalQueue(jqBridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Returns the latest journal sequence number, or {@code null} if no journal entries exist.
     *
     * @return latest sequence, or {@code null}
     * @throws TmslException if the query fails
     */
    public Long journalLatestSequence() {
        checkNotClosed();
        try {
            return bridge.journalLatestSequence();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Reads a single journal record by sequence number.
     *
     * @param sequence journal sequence number (1-based)
     * @return the record, or {@code null} if not found
     * @throws TmslException if the read fails
     */
    public JournalRecord journalRead(long sequence) {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.JournalRecord kotlinRecord = bridge.journalRead(sequence);
            return kotlinRecord != null ? new JournalRecord(kotlinRecord) : null;
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Queries journal records in the given sequence range (inclusive).
     *
     * @param startSequence start of range (inclusive)
     * @param endSequence   end of range (inclusive)
     * @return list of matching journal records
     * @throws TmslException if the query fails
     */
    public List<JournalRecord> journalQuery(long startSequence, long endSequence) {
        checkNotClosed();
        try {
            List<io.github.snower.timslite.uniffi.JournalRecord> kotlinRecords =
                    bridge.journalQuery(startSequence, endSequence);
            List<JournalRecord> result = new ArrayList<>(kotlinRecords.size());
            for (io.github.snower.timslite.uniffi.JournalRecord kr : kotlinRecords) {
                result.add(new JournalRecord(kr));
            }
            return result;
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }
}
