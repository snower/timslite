package io.github.snower.timslite;

import io.github.snower.timslite.errors.TmslException;
import java.util.ArrayList;
import java.util.List;

/**
 * A time-series dataset for reading, writing, appending, and querying records.
 *
 * <p>Each dataset is identified by a {@code (name, type)} pair and stores
 * timestamped records backed by mmap segments. Timestamps are signed 64-bit
 * values ({@code long}). Data payloads are raw {@code byte[]} arrays, capped
 * at 4 MiB per record.</p>
 *
 * <p>This class implements {@link AutoCloseable}:</p>
 *
 * <pre>{@code
 * try (Dataset ds = store.openDataset("metrics", "cpu")) {
 *     ds.write(1700000000L, new byte[]{1, 2, 3});
 *     Record rec = ds.read(1700000000L);
 * }
 * }</pre>
 *
 * <p>Individual dataset operations are not thread-safe. Synchronize externally
 * when sharing a {@code Dataset} across threads.</p>
 */
public final class Dataset implements AutoCloseable {
    private final io.github.snower.timslite.uniffi.DatasetBridge bridge;
    private boolean closed;

    Dataset(io.github.snower.timslite.uniffi.DatasetBridge bridge) {
        this.bridge = bridge;
        this.closed = false;
    }

    io.github.snower.timslite.uniffi.DatasetBridge bridge() {
        return bridge;
    }

    /** Closes this dataset handle. */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * Returns whether this dataset has been closed.
     *
     * @return {@code true} if {@link #close()} has been called
     */
    public boolean isClosed() {
        return closed;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Dataset is closed");
        }
    }

    /**
     * Writes a record at the given timestamp. If a record already exists at
     * that timestamp, a correction is applied.
     *
     * @param timestamp signed 64-bit timestamp
     * @param data      record payload, up to 4 MiB
     * @throws TmslException      if the write fails
     * @throws ExpiredException   if the timestamp falls outside the retention window
     */
    public void write(long timestamp, byte[] data) {
        checkNotClosed();
        List<kotlin.UByte> kotlinData = KotlinConversions.toUByteList(data);
        try {
            bridge.write(timestamp, kotlinData);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Appends data to the latest record or creates a new one.
     *
     * <p>If {@code timestamp} equals the latest written timestamp and the
     * latest record is an uncompressed tail, the data is appended in-place.
     * If {@code timestamp} is greater, a new record is created. If
     * {@code timestamp} is less, an error is thrown.</p>
     *
     * @param timestamp signed 64-bit timestamp
     * @param data      payload bytes to append
     * @throws TmslException if the append fails or timestamp is out of order
     */
    public void append(long timestamp, byte[] data) {
        checkNotClosed();
        List<kotlin.UByte> kotlinData = KotlinConversions.toUByteList(data);
        try {
            bridge.append(timestamp, kotlinData);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Deletes the record at the given timestamp.
     *
     * @param timestamp signed 64-bit timestamp
     * @throws TmslException    if the delete fails
     * @throws ExpiredException if the timestamp falls outside the retention window
     */
    public void delete(long timestamp) {
        checkNotClosed();
        try {
            bridge.delete(timestamp);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Reads the record at the given timestamp.
     *
     * @param timestamp signed 64-bit timestamp; pass {@code -1} to read the
     *                  latest written timestamp
     * @return the record, or {@code null} if not found, deleted, or expired
     * @throws TmslException if the read fails
     */
    public Record read(long timestamp) {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.Record kotlinRecord = bridge.read(timestamp);
            return kotlinRecord != null ? new Record(kotlinRecord) : null;
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Reads the record at the latest written timestamp.
     *
     * @return the latest record, or {@code null} if the dataset is empty or
     *         the latest record was deleted
     * @throws TmslException if the read fails
     */
    public Record readLatest() {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.Record kotlinRecord = bridge.readLatest();
            return kotlinRecord != null ? new Record(kotlinRecord) : null;
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Checks whether a record exists at the given timestamp.
     *
     * @param timestamp signed 64-bit timestamp
     * @return {@code true} if a record exists and is not deleted or expired
     * @throws TmslException if the check fails
     */
    public boolean readExist(long timestamp) {
        checkNotClosed();
        try {
            return bridge.readExist(timestamp);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Returns the byte length of the record at the given timestamp.
     *
     * @param timestamp signed 64-bit timestamp
     * @return record length in bytes, or {@code null} if not found
     * @throws TmslException if the query fails
     */
    public Integer readLength(long timestamp) {
        checkNotClosed();
        try {
            return KotlinConversions.readLengthValue(bridge, timestamp);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Queries records in the given timestamp range (inclusive).
     *
     * @param startTs start of range (inclusive)
     * @param endTs   end of range (inclusive)
     * @return list of matching records
     * @throws TmslException if the query fails
     */
    public List<Record> query(long startTs, long endTs) {
        checkNotClosed();
        try {
            List<io.github.snower.timslite.uniffi.Record> kotlinRecords = bridge.query(startTs, endTs);
            List<Record> result = new ArrayList<>(kotlinRecords.size());
            for (io.github.snower.timslite.uniffi.Record kr : kotlinRecords) {
                result.add(new Record(kr));
            }
            return result;
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Returns a bitmap indicating which timestamps in the range have records.
     *
     * @param startTs start of range (inclusive)
     * @param endTs   end of range (inclusive)
     * @return bitmap bytes; each bit represents one timestamp in order
     * @throws TmslException if the query fails
     */
    public byte[] queryExist(long startTs, long endTs) {
        checkNotClosed();
        try {
            List<kotlin.UByte> kotlinResult = bridge.queryExist(startTs, endTs);
            return KotlinConversions.fromUByteList(kotlinResult);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Queries record lengths in the given timestamp range (inclusive).
     *
     * @param startTs start of range (inclusive)
     * @param endTs   end of range (inclusive)
     * @return list of timestamp/length pairs
     * @throws TmslException if the query fails
     */
    public List<LengthEntry> queryLength(long startTs, long endTs) {
        checkNotClosed();
        try {
            List<io.github.snower.timslite.uniffi.LengthEntry> kotlinEntries = bridge.queryLength(startTs, endTs);
            List<LengthEntry> result = new ArrayList<>(kotlinEntries.size());
            for (io.github.snower.timslite.uniffi.LengthEntry ke : kotlinEntries) {
                result.add(new LengthEntry(ke));
            }
            return result;
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Returns a lazy iterator over records in the given timestamp range.
     *
     * <p>The returned iterator holds native resources and must be closed
     * when no longer needed (prefer try-with-resources).</p>
     *
     * @param startTs start of range (inclusive)
     * @param endTs   end of range (inclusive)
     * @return a query iterator
     * @throws TmslException if the iterator cannot be created
     */
    public QueryIterator queryIter(long startTs, long endTs) {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.QueryIteratorBridge iterBridge = bridge.queryIter(startTs, endTs);
            return new QueryIterator(iterBridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Returns a lazy iterator over record lengths in the given timestamp range.
     *
     * <p>The returned iterator holds native resources and must be closed
     * when no longer needed.</p>
     *
     * @param startTs start of range (inclusive)
     * @param endTs   end of range (inclusive)
     * @return a query length iterator
     * @throws TmslException if the iterator cannot be created
     */
    public QueryLengthIterator queryLengthIter(long startTs, long endTs) {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.QueryLengthIteratorBridge iterBridge = bridge.queryLengthIter(startTs, endTs);
            return new QueryLengthIterator(iterBridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Flushes pending writes to disk.
     *
     * @throws TmslException if the flush fails
     */
    public void flush() {
        checkNotClosed();
        try {
            bridge.flush();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }
}
