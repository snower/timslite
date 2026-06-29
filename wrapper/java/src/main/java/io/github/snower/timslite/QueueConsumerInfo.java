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
        this.runningExpiredSeconds = kotlinInfo.getRunningExpiredSeconds().longValue();
        this.maxRetryCount = kotlinInfo.getMaxRetryCount().intValue();
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
