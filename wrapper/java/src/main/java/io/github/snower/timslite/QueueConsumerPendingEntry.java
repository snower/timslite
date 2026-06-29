package io.github.snower.timslite;

/**
 * Pending queue record state returned by {@link QueueConsumer#inspect()}.
 */
public final class QueueConsumerPendingEntry {
    private final long timestamp;
    private final long startTime;
    private final short status;
    private final short retryCount;

    QueueConsumerPendingEntry(io.github.snower.timslite.uniffi.QueueConsumerPendingEntry kotlinEntry) {
        this.timestamp = kotlinEntry.getTimestamp();
        this.startTime = kotlinEntry.getStartTime();
        this.status = kotlinEntry.getStatus().shortValue();
        this.retryCount = kotlinEntry.getRetryCount().shortValue();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getStartTime() {
        return startTime;
    }

    public short getStatus() {
        return status;
    }

    public short getRetryCount() {
        return retryCount;
    }
}
