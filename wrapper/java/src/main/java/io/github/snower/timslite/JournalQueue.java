package io.github.snower.timslite;

import io.github.snower.timslite.errors.TmslException;
import io.github.snower.timslite.uniffi.JournalQueueBridge;
import io.github.snower.timslite.uniffi.JournalQueueConsumerBridge;
import io.github.snower.timslite.uniffi.QueueConsumerOptions;

/**
 * A queue for consuming journal change-log records.
 *
 * <p>The journal queue delivers records in sequence order. Each record
 * contains a sequence number and the serialized change payload.</p>
 *
 * <p>This class implements {@link AutoCloseable}.</p>
 */
public final class JournalQueue implements AutoCloseable {
    private final JournalQueueBridge bridge;
    private boolean closed;

    JournalQueue(JournalQueueBridge bridge) {
        this.bridge = bridge;
        this.closed = false;
    }

    /** Closes this journal queue handle. */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            bridge.close();
        }
    }

    /**
     * Returns whether this journal queue has been closed.
     *
     * @return {@code true} if {@link #close()} has been called
     */
    public boolean isClosed() {
        return closed;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("JournalQueue is closed");
        }
    }

    /**
     * Opens a journal consumer for the given group.
     *
     * @param groupName consumer group name, must match {@code ^[0-9A-Za-z_-]+$}
     * @param options   consumer configuration options
     * @return the journal consumer handle
     * @throws TmslException if the consumer cannot be opened
     */
    public JournalQueueConsumer openConsumer(String groupName, QueueConsumerOptions options) {
        checkNotClosed();
        try {
            JournalQueueConsumerBridge consumerBridge = bridge.openConsumer(groupName, options);
            return new JournalQueueConsumer(consumerBridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Opens a journal consumer for the given group with default options.
     *
     * @param groupName consumer group name
     * @return the journal consumer handle
     * @throws TmslException if the consumer cannot be opened
     */
    public JournalQueueConsumer openConsumer(String groupName) {
        return openConsumer(groupName, QueueConsumerOptionsBuilder.builder().build());
    }
}
