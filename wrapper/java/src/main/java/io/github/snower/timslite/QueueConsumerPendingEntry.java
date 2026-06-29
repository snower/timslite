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
        this.status = (short) (KotlinConversions.getUByte(kotlinEntry, "getStatus") & 0xFF);
        this.retryCount = (short) (KotlinConversions.getUByte(kotlinEntry, "getRetryCount") & 0xFF);
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
