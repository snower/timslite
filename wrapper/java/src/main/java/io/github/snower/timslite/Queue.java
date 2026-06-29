package io.github.snower.timslite;

import io.github.snower.timslite.errors.TmslException;
import io.github.snower.timslite.uniffi.QueueBridge;
import io.github.snower.timslite.uniffi.QueueConsumerBridge;
import io.github.snower.timslite.uniffi.QueueConsumerOptions;
import java.util.List;

/**
 * A persistent queue backed by a dataset.
 *
 * <p>Records pushed to the queue are durably stored and can be consumed
 * by one or more consumer groups. Each consumer group tracks its own
 * position independently.</p>
 *
 * <p>This class implements {@link AutoCloseable}.</p>
 */
public final class Queue implements AutoCloseable {
    private final QueueBridge bridge;
    private boolean closed;

    Queue(QueueBridge bridge) {
        this.bridge = bridge;
        this.closed = false;
    }

    /** Closes this queue handle. */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                bridge.release();
            } catch (io.github.snower.timslite.uniffi.TmslException e) {
                throw TmslException.fromUniFFI(e);
            }
        }
    }

    /**
     * Returns whether this queue has been closed.
     *
     * @return {@code true} if {@link #close()} has been called
     */
    public boolean isClosed() {
        return closed;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Queue is closed");
        }
    }

    /**
     * Pushes a record to the queue.
     *
     * @param data record payload
     * @return the timestamp assigned to this record
     * @throws TmslException if the push fails
     */
    public long push(byte[] data) {
        checkNotClosed();
        List<kotlin.UByte> kotlinData = KotlinConversions.toUByteList(data);
        try {
            return bridge.push(kotlinData);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Opens a consumer for the given group.
     *
     * @param groupName consumer group name, must match {@code ^[0-9A-Za-z_-]+$}
     * @param options   consumer configuration options
     * @return the consumer handle
     * @throws TmslException if the consumer cannot be opened
     */
    public QueueConsumer openConsumer(String groupName, QueueConsumerOptions options) {
        checkNotClosed();
        try {
            QueueConsumerBridge consumerBridge = bridge.openConsumer(groupName, options);
            return new QueueConsumer(consumerBridge);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Opens a consumer for the given group with default options.
     *
     * @param groupName consumer group name
     * @return the consumer handle
     * @throws TmslException if the consumer cannot be opened
     */
    public QueueConsumer openConsumer(String groupName) {
        return openConsumer(groupName, QueueConsumerOptionsBuilder.builder().build());
    }

    /**
     * Returns current consumer group names.
     *
     * <p>This lists existing state file directory entries without opening or
     * validating the state files.</p>
     *
     * @return consumer group names
     * @throws TmslException if the list operation fails
     */
    public List<String> getConsumerGroupNames() {
        checkNotClosed();
        try {
            return bridge.getConsumerGroupNames();
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }

    /**
     * Drops a consumer group and its state.
     *
     * @param groupName consumer group name to drop
     * @throws TmslException if the group does not exist or cannot be dropped
     */
    public void dropConsumer(String groupName) {
        checkNotClosed();
        try {
            bridge.dropConsumer(groupName);
        } catch (io.github.snower.timslite.uniffi.TmslException e) {
            throw TmslException.fromUniFFI(e);
        }
    }
}
