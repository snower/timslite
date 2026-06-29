package io.github.snower.timslite;

/**
 * Public configuration for a queue consumer group.
 */
public final class QueueConsumerInfo {
    private final String groupName;
    private final long runningExpiredSeconds;
    private final int maxRetryCount;

    QueueConsumerInfo(io.github.snower.timslite.uniffi.QueueConsumerInfo kotlinInfo) {
        this.groupName = kotlinInfo.getGroupName();
        this.runningExpiredSeconds = KotlinConversions.getULong(kotlinInfo, "getRunningExpiredSeconds");
        this.maxRetryCount = (short) (KotlinConversions.getUShort(kotlinInfo, "getMaxRetryCount") & 0xFFFF);
    }

    public String getGroupName() {
        return groupName;
    }

    public long getRunningExpiredSeconds() {
        return runningExpiredSeconds;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }
}
