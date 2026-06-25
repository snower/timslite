package io.github.snower.timslite;

import io.github.snower.timslite.uniffi.DatasetConfig;

/**
 * Builder for {@link DatasetConfig}.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * DatasetConfig config = DatasetConfigBuilder.builder()
 *         .indexContinuous((byte) 0)
 *         .retentionWindow(0)
 *         .build();
 * }</pre>
 */
public final class DatasetConfigBuilder {
    private Long dataSegmentSize;
    private Long indexSegmentSize;
    private Long initialDataSegmentSize;
    private Long initialIndexSegmentSize;
    private Byte compressLevel;
    private Byte compressType;
    private Byte indexContinuous;
    private Long retentionWindow;
    private Boolean enableJournal;

    private DatasetConfigBuilder() {
    }

    /**
     * Creates a new builder with default values.
     *
     * @return a new builder
     */
    public static DatasetConfigBuilder builder() {
        return new DatasetConfigBuilder();
    }

    /**
     * Sets the data segment size in bytes.
     *
     * @param dataSegmentSize segment size, must be non-negative
     * @return this builder
     */
    public DatasetConfigBuilder dataSegmentSize(long dataSegmentSize) {
        if (dataSegmentSize < 0) {
            throw new IllegalArgumentException("dataSegmentSize must be non-negative, got " + dataSegmentSize);
        }
        this.dataSegmentSize = dataSegmentSize;
        return this;
    }

    /**
     * Sets the index segment size in bytes.
     *
     * @param indexSegmentSize segment size, must be non-negative
     * @return this builder
     */
    public DatasetConfigBuilder indexSegmentSize(long indexSegmentSize) {
        if (indexSegmentSize < 0) {
            throw new IllegalArgumentException("indexSegmentSize must be non-negative, got " + indexSegmentSize);
        }
        this.indexSegmentSize = indexSegmentSize;
        return this;
    }

    /**
     * Sets the initial data segment size in bytes.
     *
     * @param initialDataSegmentSize initial size, must be non-negative
     * @return this builder
     */
    public DatasetConfigBuilder initialDataSegmentSize(long initialDataSegmentSize) {
        if (initialDataSegmentSize < 0) {
            throw new IllegalArgumentException("initialDataSegmentSize must be non-negative, got " + initialDataSegmentSize);
        }
        this.initialDataSegmentSize = initialDataSegmentSize;
        return this;
    }

    /**
     * Sets the initial index segment size in bytes.
     *
     * @param initialIndexSegmentSize initial size, must be non-negative
     * @return this builder
     */
    public DatasetConfigBuilder initialIndexSegmentSize(long initialIndexSegmentSize) {
        if (initialIndexSegmentSize < 0) {
            throw new IllegalArgumentException("initialIndexSegmentSize must be non-negative, got " + initialIndexSegmentSize);
        }
        this.initialIndexSegmentSize = initialIndexSegmentSize;
        return this;
    }

    /**
     * Sets the compression level.
     *
     * @param compressLevel compression level
     * @return this builder
     */
    public DatasetConfigBuilder compressLevel(byte compressLevel) {
        this.compressLevel = compressLevel;
        return this;
    }

    /**
     * Sets the compression type (0 = none, 1 = deflate, 2 = zstd).
     *
     * @param compressType compression type
     * @return this builder
     */
    public DatasetConfigBuilder compressType(byte compressType) {
        this.compressType = compressType;
        return this;
    }

    /**
     * Sets the index mode. 0 = sparse, 1 = continuous.
     *
     * @param indexContinuous index mode
     * @return this builder
     */
    public DatasetConfigBuilder indexContinuous(byte indexContinuous) {
        this.indexContinuous = indexContinuous;
        return this;
    }

    /**
     * Sets the retention window in the dataset's timestamp unit.
     * 0 means no retention limit.
     *
     * @param retentionWindow retention window, must be non-negative
     * @return this builder
     */
    public DatasetConfigBuilder retentionWindow(long retentionWindow) {
        if (retentionWindow < 0) {
            throw new IllegalArgumentException("retentionWindow must be non-negative, got " + retentionWindow);
        }
        this.retentionWindow = retentionWindow;
        return this;
    }

    /**
     * Enables or disables journal for this dataset.
     *
     * @param enableJournal {@code true} to enable
     * @return this builder
     */
    public DatasetConfigBuilder enableJournal(boolean enableJournal) {
        this.enableJournal = enableJournal;
        return this;
    }

    /**
     * Builds the {@link DatasetConfig}.
     *
     * @return the built config
     */
    public DatasetConfig build() {
        return UniFFIBridge.buildDatasetConfig(
                dataSegmentSize,
                indexSegmentSize,
                initialDataSegmentSize,
                initialIndexSegmentSize,
                compressLevel,
                compressType,
                indexContinuous,
                retentionWindow,
                enableJournal
        );
    }
}
