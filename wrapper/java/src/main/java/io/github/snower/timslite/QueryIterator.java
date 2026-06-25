package io.github.snower.timslite;

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

    /** Closes this iterator and releases native resources. */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            bridge.close();
        }
    }
}
