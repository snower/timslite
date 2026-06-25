package io.github.snower.timslite;

import io.github.snower.timslite.uniffi.QueueConsumerConfig;
import io.github.snower.timslite.uniffi.QueueConsumerOptions;

/**
 * Builder for {@link QueueConsumerOptions}.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * QueueConsumerOptions options = QueueConsumerOptionsBuilder.builder()
 *         .config(QueueConsumerConfigBuilder.builder().build())
 *         .build();
 * }</pre>
 */
public final class QueueConsumerOptionsBuilder {
    private QueueConsumerConfig config;

    private QueueConsumerOptionsBuilder() {
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder
     */
    public static QueueConsumerOptionsBuilder builder() {
        return new QueueConsumerOptionsBuilder();
    }

    /**
     * Sets the consumer configuration.
     *
     * @param config consumer config
     * @return this builder
     */
    public QueueConsumerOptionsBuilder config(QueueConsumerConfig config) {
        this.config = config;
        return this;
    }

    /**
     * Builds the {@link QueueConsumerOptions}.
     *
     * @return the built options
     */
    public QueueConsumerOptions build() {
        return UniFFIBridge.buildQueueConsumerOptions(config);
    }
}
