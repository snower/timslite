package io.github.snower.timslite;

/**
 * Iterator over query-length results. Implements AutoCloseable to release native resources.
 *
 * <p>Typical usage with try-with-resources:</p>
 * <pre>{@code
 * try (QueryLengthIterator it = ds.queryLengthIter(startTs, endTs)) {
 *     while (it.hasNext()) {
 *         LengthEntry entry = it.next();
 *         // process entry
 *     }
 * }
 * }</pre>
 */
public final class QueryLengthIterator implements AutoCloseable {
    private final io.github.snower.timslite.uniffi.QueryLengthIteratorBridge bridge;
    private LengthEntry nextEntry;
    private boolean closed;
    private boolean exhausted;

    QueryLengthIterator(io.github.snower.timslite.uniffi.QueryLengthIteratorBridge bridge) {
        this.bridge = bridge;
        this.exhausted = false;
        this.closed = false;
        this.nextEntry = advance();
    }

    private LengthEntry advance() {
        if (exhausted) {
            return null;
        }
        io.github.snower.timslite.uniffi.LengthEntry kotlinEntry;
        try {
            kotlinEntry = bridge.next();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw io.github.snower.timslite.errors.TmslException.fromUniFFI(e);
        }
        if (kotlinEntry == null) {
            exhausted = true;
            return null;
        }
        return new LengthEntry(kotlinEntry);
    }

    /**
     * Returns {@code true} if more entries are available.
     *
     * @return whether {@link #next()} will return an entry
     */
    public boolean hasNext() {
        return !closed && !exhausted && nextEntry != null;
    }

    /**
     * Returns the next length entry in the iteration.
     *
     * @return the next entry
     * @throws IllegalStateException if the iterator is closed
     */
    public LengthEntry next() {
        if (closed) {
            throw new IllegalStateException("QueryLengthIterator is closed");
        }
        LengthEntry current = nextEntry;
        nextEntry = advance();
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
