package io.github.snower.timslite;

import io.github.snower.timslite.uniffi.StoreConfig;

/**
 * Builder for {@link StoreConfig}.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * StoreConfig config = StoreConfigBuilder.builder()
 *         .enableJournal(true)
 *         .enableBackgroundThread(true)
 *         .build();
 * }</pre>
 */
public final class StoreConfigBuilder {
    private Long flushIntervalSecs;
    private Long idleTimeoutSecs;
    private Long dataSegmentSize;
    private Long indexSegmentSize;
    private Long initialDataSegmentSize;
    private Long initialIndexSegmentSize;
    private Byte compressLevel;
    private Long cacheMaxMemory;
    private Long cacheIdleTimeoutSecs;
    private Byte retentionCheckHour;
    private Boolean enableBackgroundThread;
    private Boolean enableJournal;
    private Boolean readOnly;

    private StoreConfigBuilder() {
    }

    /**
     * Creates a new builder with default values.
     *
     * @return a new builder
     */
    public static StoreConfigBuilder builder() {
        return new StoreConfigBuilder();
    }

    /**
     * Sets the flush interval in seconds. 0 disables periodic flushing.
     *
     * @param flushIntervalSecs flush interval, must be non-negative
     * @return this builder
     */
    public StoreConfigBuilder flushIntervalSecs(long flushIntervalSecs) {
        validateNonNegative(flushIntervalSecs, "flushIntervalSecs");
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    /**
     * Sets the idle timeout in seconds before a segment is closed.
     *
     * @param idleTimeoutSecs idle timeout, must be non-negative
     * @return this builder
     */
    public StoreConfigBuilder idleTimeoutSecs(long idleTimeoutSecs) {
        validateNonNegative(idleTimeoutSecs, "idleTimeoutSecs");
        this.idleTimeoutSecs = idleTimeoutSecs;
        return this;
    }

    /**
     * Sets the data segment size in bytes.
     *
     * @param dataSegmentSize segment size, must be non-negative
     * @return this builder
     */
    public StoreConfigBuilder dataSegmentSize(long dataSegmentSize) {
        validateNonNegative(dataSegmentSize, "dataSegmentSize");
        this.dataSegmentSize = dataSegmentSize;
        return this;
    }

    /**
     * Sets the index segment size in bytes.
     *
     * @param indexSegmentSize segment size, must be non-negative
     * @return this builder
     */
    public StoreConfigBuilder indexSegmentSize(long indexSegmentSize) {
        validateNonNegative(indexSegmentSize, "indexSegmentSize");
        this.indexSegmentSize = indexSegmentSize;
        return this;
    }

    /**
     * Sets the initial data segment size in bytes.
     *
     * @param initialDataSegmentSize initial size, must be non-negative
     * @return this builder
     */
    public StoreConfigBuilder initialDataSegmentSize(long initialDataSegmentSize) {
        validateNonNegative(initialDataSegmentSize, "initialDataSegmentSize");
        this.initialDataSegmentSize = initialDataSegmentSize;
        return this;
    }

    /**
     * Sets the initial index segment size in bytes.
     *
     * @param initialIndexSegmentSize initial size, must be non-negative
     * @return this builder
     */
    public StoreConfigBuilder initialIndexSegmentSize(long initialIndexSegmentSize) {
        validateNonNegative(initialIndexSegmentSize, "initialIndexSegmentSize");
        this.initialIndexSegmentSize = initialIndexSegmentSize;
        return this;
    }

    /**
     * Sets the compression level (0-9 for deflate, 0-22 for zstd).
     *
     * @param compressLevel compression level
     * @return this builder
     */
    public StoreConfigBuilder compressLevel(byte compressLevel) {
        this.compressLevel = compressLevel;
        return this;
    }

    /**
     * Sets the maximum memory for the block cache in bytes.
     *
     * @param cacheMaxMemory max memory, must be non-negative
     * @return this builder
     */
    public StoreConfigBuilder cacheMaxMemory(long cacheMaxMemory) {
        validateNonNegative(cacheMaxMemory, "cacheMaxMemory");
        this.cacheMaxMemory = cacheMaxMemory;
        return this;
    }

    /**
     * Sets the idle timeout for cache eviction in seconds.
     *
     * @param cacheIdleTimeoutSecs idle timeout, must be non-negative
     * @return this builder
     */
    public StoreConfigBuilder cacheIdleTimeoutSecs(long cacheIdleTimeoutSecs) {
        validateNonNegative(cacheIdleTimeoutSecs, "cacheIdleTimeoutSecs");
        this.cacheIdleTimeoutSecs = cacheIdleTimeoutSecs;
        return this;
    }

    /**
     * Sets the UTC hour for retention checks (0-23).
     *
     * @param retentionCheckHour hour of day
     * @return this builder
     */
    public StoreConfigBuilder retentionCheckHour(byte retentionCheckHour) {
        this.retentionCheckHour = retentionCheckHour;
        return this;
    }

    /**
     * Enables or disables the background task thread.
     *
     * @param enableBackgroundThread {@code true} to enable
     * @return this builder
     */
    public StoreConfigBuilder enableBackgroundThread(boolean enableBackgroundThread) {
        this.enableBackgroundThread = enableBackgroundThread;
        return this;
    }

    /**
     * Enables or disables the journal.
     *
     * @param enableJournal {@code true} to enable
     * @return this builder
     */
    public StoreConfigBuilder enableJournal(boolean enableJournal) {
        this.enableJournal = enableJournal;
        return this;
    }

    /**
     * Opens the store in read-only mode.
     *
     * @param readOnly {@code true} for read-only
     * @return this builder
     */
    public StoreConfigBuilder readOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    /**
     * Builds the {@link StoreConfig}.
     *
     * @return the built config
     */
    public StoreConfig build() {
        return UniFFIBridge.buildStoreConfig(
                flushIntervalSecs,
                idleTimeoutSecs,
                dataSegmentSize,
                indexSegmentSize,
                initialDataSegmentSize,
                initialIndexSegmentSize,
                compressLevel,
                cacheMaxMemory,
                cacheIdleTimeoutSecs,
                retentionCheckHour,
                enableBackgroundThread,
                enableJournal,
                readOnly
        );
    }

    private static void validateNonNegative(long value, String fieldName) {
        if (value < 0) {
            throw new IllegalArgumentException(fieldName + " must be non-negative, got " + value);
        }
    }
}
