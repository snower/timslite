package io.github.snower.timslite;

import io.github.snower.timslite.errors.TmslException;
import io.github.snower.timslite.uniffi.JournalQueueConsumerBridge;

/**
 * A consumer that reads journal change-log records within a specific consumer group.
 *
 * <p>{@code poll()} blocks until a record is available or the timeout expires.
 * After processing a record, call {@link #ack(long)} to advance the consumer
 * position. Unacknowledged records may be redelivered on the next poll.</p>
 *
 * <p>This class implements {@link AutoCloseable}.</p>
 */
public final class JournalQueueConsumer implements AutoCloseable {
    private static final java.lang.reflect.Method POLL_METHOD;

    static {
        try {
            POLL_METHOD = JournalQueueConsumerBridge.class.getMethod("poll-VKZWuLQ", long.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find JournalQueueConsumerBridge.poll method", e);
        }
    }

    private final JournalQueueConsumerBridge bridge;
    private boolean closed;

    JournalQueueConsumer(JournalQueueConsumerBridge bridge) {
        this.bridge = bridge;
        this.closed = false;
    }

    /** Closes this journal consumer handle. */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            bridge.close();
        }
    }

    /**
     * Returns whether this journal consumer has been closed.
     *
     * @return {@code true} if {@link #close()} has been called
     */
    public boolean isClosed() {
        return closed;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("JournalQueueConsumer is closed");
        }
    }

    /**
     * Polls for the next journal record, blocking up to the specified timeout.
     *
     * @param timeoutMs maximum time to wait in milliseconds; 0 returns
     *                  immediately if no record is available
     * @return the next journal record, or {@code null} if the timeout expires
     * @throws TmslException if polling fails
     */
    public JournalRecord poll(long timeoutMs) {
        checkNotClosed();
        try {
            io.github.snower.timslite.uniffi.JournalRecord kotlinRecord =
                    (io.github.snower.timslite.uniffi.JournalRecord) POLL_METHOD.invoke(bridge, timeoutMs);
            return kotlinRecord != null ? new JournalRecord(kotlinRecord) : null;
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof io.github.snower.timslite.uniffi.TmslException) {
                throw TmslException.fromUniFFI((io.github.snower.timslite.uniffi.TmslException) cause);
            }
            throw new RuntimeException(cause);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to invoke poll", e);
        }
    }

    /**
     * Acknowledges a journal record, advancing the consumer position past its sequence.
     *
     * @param sequence the journal sequence number to acknowledge
     * @throws TmslException if the acknowledgment fails
     */
    public void ack(long sequence) {
        checkNotClosed();
        try {
            bridge.ack(sequence);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }
}
