package io.github.snower.timslite;

import java.util.ArrayList;
import java.util.List;

/**
 * Iterator over query results. Implements AutoCloseable to release native resources.
 *
 * <p>Typical usage with try-with-resources:</p>
 * <pre>{@code
 * try (QueryIterator it = ds.queryIter(startTs, endTs)) {
 *     while (it.hasNext()) {
 *         Record rec = it.next();
 *         // process record
 *     }
 * }
 * }</pre>
 */
public final class QueryIterator implements AutoCloseable {
    private final io.github.snower.timslite.uniffi.QueryIteratorBridge bridge;
    private Record nextRecord;
    private boolean closed;
    private boolean exhausted;

    QueryIterator(io.github.snower.timslite.uniffi.QueryIteratorBridge bridge) {
        this.bridge = bridge;
        this.exhausted = false;
        this.closed = false;
        this.nextRecord = advance();
    }

    private Record advance() {
        if (exhausted) {
            return null;
        }
        io.github.snower.timslite.uniffi.Record kotlinRecord;
        try {
            kotlinRecord = bridge.next();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw io.github.snower.timslite.errors.TmslException.fromUniFFI(e);
        }
        if (kotlinRecord == null) {
            exhausted = true;
            return null;
        }
        return new Record(kotlinRecord);
    }

    /**
     * Returns {@code true} if more records are available.
     *
     * @return whether {@link #next()} will return a record
     */
    public boolean hasNext() {
        return !closed && !exhausted && nextRecord != null;
    }

    /**
     * Returns the next record in the iteration.
     *
     * @return the next record
     * @throws IllegalStateException if the iterator is closed
     */
    public Record next() {
        if (closed) {
            throw new IllegalStateException("QueryIterator is closed");
        }
        Record current = nextRecord;
        nextRecord = advance();
        return current;
    }

    /**
     * Reverses the iteration direction.
     *
     * @throws IllegalStateException if the iterator is closed
     */
    public void reverse() {
        if (closed) {
            throw new IllegalStateException("QueryIterator is closed");
        }
        try {
            bridge.reverse();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw io.github.snower.timslite.errors.TmslException.fromUniFFI(e);
        }
        exhausted = false;
        nextRecord = advance();
    }

    /**
     * Skips the next count entries.
     *
     * @param count number of entries to skip
     * @throws IllegalStateException if the iterator is closed
     */
    public void skip(int count) {
        if (closed) {
            throw new IllegalStateException("QueryIterator is closed");
        }
        try {
            KotlinConversions.callSkip(bridge, count);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof io.github.snower.timslite.uniffi.TmslException) {
                throw io.github.snower.timslite.errors.TmslException.fromUniFFI(
                    (io.github.snower.timslite.uniffi.TmslException) e.getCause());
            }
            throw e;
        }
        nextRecord = advance();
    }

    /**
     * Collects all remaining records into a list.
     * The iterator is consumed and closed after this call.
     *
     * @return list of all remaining records
     */
    public List<Record> collectAll() {
        if (closed) {
            throw new IllegalStateException("QueryIterator is closed");
        }
        List<Record> result = new ArrayList<>();
        if (nextRecord != null) {
            result.add(nextRecord);
            nextRecord = null;
        }
        try {
            List<io.github.snower.timslite.uniffi.Record> kotlinRecords = bridge.collectAll();
            for (io.github.snower.timslite.uniffi.Record kr : kotlinRecords) {
                result.add(new Record(kr));
            }
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw io.github.snower.timslite.errors.TmslException.fromUniFFI(e);
        }
        exhausted = true;
        return result;
    }

    /**
     * Collects up to count records into a list.
     * The iterator is consumed and closed after this call.
     *
     * @param count maximum number of records to collect
     * @return list of collected records
     */
    public List<Record> collectTake(int count) {
        if (closed) {
            throw new IllegalStateException("QueryIterator is closed");
        }
        List<Record> result = new ArrayList<>();
        if (nextRecord != null && count > 0) {
            result.add(nextRecord);
            nextRecord = null;
            count--;
        }
        if (count > 0) {
            try {
                List<io.github.snower.timslite.uniffi.Record> kotlinRecords =
                    KotlinConversions.callCollectTake(bridge, count);
                for (io.github.snower.timslite.uniffi.Record kr : kotlinRecords) {
                    result.add(new Record(kr));
                }
            } catch (RuntimeException e) {
                if (e.getCause() instanceof io.github.snower.timslite.uniffi.TmslException) {
                    throw io.github.snower.timslite.errors.TmslException.fromUniFFI(
                        (io.github.snower.timslite.uniffi.TmslException) e.getCause());
                }
                throw e;
            }
        }
        exhausted = true;
        return result;
    }

    /** Closes this iterator and releases native resources. */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            bridge.close();
        }
    }
}
