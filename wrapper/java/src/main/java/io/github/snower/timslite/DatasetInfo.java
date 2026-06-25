package io.github.snower.timslite;

/**
 * Configuration and metadata about a dataset.
 *
 * <p>Returned as part of {@link InspectResult}.</p>
 */
public final class DatasetInfo {
    private final String name;
    private final String datasetType;
    private final String baseDir;
    private final long identifier;
    private final long dataSegmentSize;
    private final long indexSegmentSize;
    private final long initialDataSegmentSize;
    private final long initialIndexSegmentSize;
    private final short compressType;
    private final short compressLevel;
    private final short indexContinuous;
    private final long retentionWindow;
    private final boolean enableJournal;
    private final long createTime;

    DatasetInfo(io.github.snower.timslite.uniffi.DataSetInfo kotlinInfo) {
        this.name = kotlinInfo.getName();
        this.datasetType = kotlinInfo.getDatasetType();
        this.baseDir = kotlinInfo.getBaseDir();
        this.identifier = KotlinConversions.getULong(kotlinInfo, "getIdentifier");
        this.dataSegmentSize = KotlinConversions.getULong(kotlinInfo, "getDataSegmentSize");
        this.indexSegmentSize = KotlinConversions.getULong(kotlinInfo, "getIndexSegmentSize");
        this.initialDataSegmentSize = KotlinConversions.getULong(kotlinInfo, "getInitialDataSegmentSize");
        this.initialIndexSegmentSize = KotlinConversions.getULong(kotlinInfo, "getInitialIndexSegmentSize");
        this.compressType = (short) (KotlinConversions.getUByte(kotlinInfo, "getCompressType") & 0xFF);
        this.compressLevel = (short) (KotlinConversions.getUByte(kotlinInfo, "getCompressLevel") & 0xFF);
        this.indexContinuous = (short) (KotlinConversions.getUByte(kotlinInfo, "getIndexContinuous") & 0xFF);
        this.retentionWindow = KotlinConversions.getULong(kotlinInfo, "getRetentionWindow");
        this.enableJournal = kotlinInfo.getEnableJournal();
        this.createTime = kotlinInfo.getCreateTime();
    }

    /**
     * Returns the dataset name.
     *
     * @return dataset name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the dataset type.
     *
     * @return dataset type
     */
    public String getDatasetType() {
        return datasetType;
    }

    /**
     * Returns the base directory path for this dataset's files.
     *
     * @return base directory path
     */
    public String getBaseDir() {
        return baseDir;
    }

    /**
     * Returns the numeric identifier for this dataset.
     *
     * @return dataset identifier
     */
    public long getIdentifier() {
        return identifier;
    }

    /**
     * Returns the configured data segment size in bytes.
     *
     * @return data segment size
     */
    public long getDataSegmentSize() {
        return dataSegmentSize;
    }

    /**
     * Returns the configured index segment size in bytes.
     *
     * @return index segment size
     */
    public long getIndexSegmentSize() {
        return indexSegmentSize;
    }

    /**
     * Returns the initial data segment size in bytes.
     *
     * @return initial data segment size
     */
    public long getInitialDataSegmentSize() {
        return initialDataSegmentSize;
    }

    /**
     * Returns the initial index segment size in bytes.
     *
     * @return initial index segment size
     */
    public long getInitialIndexSegmentSize() {
        return initialIndexSegmentSize;
    }

    /**
     * Returns the compression type (0 = none, 1 = deflate, 2 = zstd).
     *
     * @return compression type
     */
    public short getCompressType() {
        return compressType;
    }

    /**
     * Returns the compression level.
     *
     * @return compression level
     */
    public short getCompressLevel() {
        return compressLevel;
    }

    /**
     * Returns whether the index mode is continuous (1) or sparse (0).
     *
     * @return index mode
     */
    public short getIndexContinuous() {
        return indexContinuous;
    }

    /**
     * Returns the retention window in the dataset's timestamp unit.
     * A value of 0 means no retention limit.
     *
     * @return retention window
     */
    public long getRetentionWindow() {
        return retentionWindow;
    }

    /**
     * Returns whether journal is enabled for this dataset.
     *
     * @return {@code true} if journal is enabled
     */
    public boolean isEnableJournal() {
        return enableJournal;
    }

    /**
     * Returns the creation time as a Unix timestamp.
     *
     * @return creation time
     */
    public long getCreateTime() {
        return createTime;
    }
}
