package io.github.snower.timslite;

import java.util.ArrayList;
import java.util.List;

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
        this.nextEntry = null;
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
        if (closed || exhausted) {
            return false;
        }
        if (nextEntry == null) {
            nextEntry = advance();
        }
        return nextEntry != null;
    }

    /**
     * Returns the next length entry in the iteration.
     *
     * @return the next entry, or {@code null} if exhausted
     * @throws IllegalStateException if the iterator is closed
     */
    public LengthEntry next() {
        if (closed) {
            throw new IllegalStateException("QueryLengthIterator is closed");
        }
        if (nextEntry == null) {
            nextEntry = advance();
        }
        LengthEntry current = nextEntry;
        nextEntry = null;
        return current;
    }

    /**
     * Reverses the iteration direction.
     *
     * @return this iterator for chaining
     * @throws IllegalStateException if the iterator is closed
     */
    public QueryLengthIterator reverse() {
        if (closed) {
            throw new IllegalStateException("QueryLengthIterator is closed");
        }
        try {
            bridge.reverse();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw io.github.snower.timslite.errors.TmslException.fromUniFFI(e);
        }
        exhausted = false;
        nextEntry = advance();
        return this;
    }

    /**
     * Skips the next count entries.
     *
     * @param count number of entries to skip
     * @return this iterator for chaining
     * @throws IllegalStateException if the iterator is closed
     */
    public QueryLengthIterator skip(int count) {
        if (closed) {
            throw new IllegalStateException("QueryLengthIterator is closed");
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
        nextEntry = null;
        return this;
    }

    /**
     * Collects all remaining length entries into a list.
     * The iterator is consumed and closed after this call.
     *
     * @return list of all remaining entries
     */
    public List<LengthEntry> collectAll() {
        if (closed) {
            throw new IllegalStateException("QueryLengthIterator is closed");
        }
        List<LengthEntry> result = new ArrayList<>();
        if (nextEntry != null) {
            result.add(nextEntry);
            nextEntry = null;
        }
        try {
            List<io.github.snower.timslite.uniffi.LengthEntry> kotlinEntries = bridge.collectAll();
            for (io.github.snower.timslite.uniffi.LengthEntry ke : kotlinEntries) {
                result.add(new LengthEntry(ke));
            }
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw io.github.snower.timslite.errors.TmslException.fromUniFFI(e);
        }
        exhausted = true;
        return result;
    }

    /**
     * Collects up to count length entries into a list.
     * The iterator is consumed and closed after this call.
     *
     * @param count maximum number of entries to collect
     * @return list of collected entries
     */
    public List<LengthEntry> collectTake(int count) {
        if (closed) {
            throw new IllegalStateException("QueryLengthIterator is closed");
        }
        List<LengthEntry> result = new ArrayList<>();
        if (nextEntry != null && count > 0) {
            result.add(nextEntry);
            nextEntry = null;
            count--;
        }
        if (count > 0) {
            try {
                List<io.github.snower.timslite.uniffi.LengthEntry> kotlinEntries =
                    KotlinConversions.callCollectTake(bridge, count);
                for (io.github.snower.timslite.uniffi.LengthEntry ke : kotlinEntries) {
                    result.add(new LengthEntry(ke));
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
