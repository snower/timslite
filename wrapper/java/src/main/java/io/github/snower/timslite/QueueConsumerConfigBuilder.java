package io.github.snower.timslite;

import io.github.snower.timslite.uniffi.QueueConsumerConfig;

/**
 * Builder for {@link QueueConsumerConfig}.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * QueueConsumerConfig config = QueueConsumerConfigBuilder.builder()
 *         .runningExpiredSeconds(30)
 *         .maxRetryCount((short) 3)
 *         .build();
 * }</pre>
 */
public final class QueueConsumerConfigBuilder {
    private Long runningExpiredSeconds;
    private Short maxRetryCount;

    private QueueConsumerConfigBuilder() {
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static QueueConsumerConfigBuilder builder() {
        return new QueueConsumerConfigBuilder();
    }

    /**
     * Sets the time in seconds before a running record is considered expired.
     *
     * @param runningExpiredSeconds expiry time, must be non-negative
     * @return this builder
     */
    public QueueConsumerConfigBuilder runningExpiredSeconds(long runningExpiredSeconds) {
        if (runningExpiredSeconds < 0) {
            throw new IllegalArgumentException("runningExpiredSeconds must be non-negative, got " + runningExpiredSeconds);
        }
        this.runningExpiredSeconds = runningExpiredSeconds;
        return this;
    }

    /**
     * Sets the maximum retry count for expired records.
     *
     * @param maxRetryCount retry count, must be non-negative
     * @return this builder
     */
    public QueueConsumerConfigBuilder maxRetryCount(short maxRetryCount) {
        if (maxRetryCount < 0) {
            throw new IllegalArgumentException("maxRetryCount must be non-negative, got " + maxRetryCount);
        }
        this.maxRetryCount = maxRetryCount;
        return this;
    }

    /**
     * Builds the {@link QueueConsumerConfig}.
     *
     * @return the built config
     */
    public QueueConsumerConfig build() {
        return UniFFIBridge.buildQueueConsumerConfig(
                runningExpiredSeconds,
                maxRetryCount
        );
    }
}
