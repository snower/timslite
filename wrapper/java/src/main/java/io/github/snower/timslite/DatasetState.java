package io.github.snower.timslite;

/**
 * Runtime state and statistics of a dataset.
 *
 * <p>Returned as part of {@link InspectResult}.</p>
 */
public final class DatasetState {
    private final Long latestWrittenTimestamp;
    private final int openDataSegments;
    private final int dataSegments;
    private final long totalRecordCount;
    private final long totalDataSize;
    private final long totalUncompressedSize;
    private final long totalInvalidRecordCount;
    private final Long minTimestamp;
    private final Long maxTimestamp;
    private final int openIndexSegments;
    private final int indexSegments;
    private final int pendingIndexEntries;
    private final Long baseTimestamp;
    private final boolean readOnly;
    private final boolean hasBlockCache;
    private final boolean hasJournal;
    private final boolean hasQueue;
    private final int queueConsumerGroups;

    DatasetState(io.github.snower.timslite.uniffi.DataSetState kotlinState) {
        Long latestTs = kotlinState.getLatestWrittenTimestamp();
        this.latestWrittenTimestamp = latestTs != null ? latestTs : null;
        this.openDataSegments = KotlinConversions.getUInt(kotlinState, "getOpenDataSegments");
        this.dataSegments = KotlinConversions.getUInt(kotlinState, "getDataSegments");
        this.totalRecordCount = KotlinConversions.getULong(kotlinState, "getTotalRecordCount");
        this.totalDataSize = KotlinConversions.getULong(kotlinState, "getTotalDataSize");
        this.totalUncompressedSize = KotlinConversions.getULong(kotlinState, "getTotalUncompressedSize");
        this.totalInvalidRecordCount = KotlinConversions.getULong(kotlinState, "getTotalInvalidRecordCount");
        Long minTs = kotlinState.getMinTimestamp();
        this.minTimestamp = minTs != null ? minTs : null;
        Long maxTs = kotlinState.getMaxTimestamp();
        this.maxTimestamp = maxTs != null ? maxTs : null;
        this.openIndexSegments = KotlinConversions.getUInt(kotlinState, "getOpenIndexSegments");
        this.indexSegments = KotlinConversions.getUInt(kotlinState, "getIndexSegments");
        this.pendingIndexEntries = KotlinConversions.getUInt(kotlinState, "getPendingIndexEntries");
        Long baseTs = kotlinState.getBaseTimestamp();
        this.baseTimestamp = baseTs != null ? baseTs : null;
        this.readOnly = kotlinState.getReadOnly();
        this.hasBlockCache = kotlinState.getHasBlockCache();
        this.hasJournal = kotlinState.getHasJournal();
        this.hasQueue = kotlinState.getHasQueue();
        this.queueConsumerGroups = KotlinConversions.getUInt(kotlinState, "getQueueConsumerGroups");
    }

    /**
     * Returns the latest successfully written timestamp, or {@code null} if
     * no data has been written.
     *
     * @return latest written timestamp, or {@code null}
     */
    public Long getLatestWrittenTimestamp() {
        return latestWrittenTimestamp;
    }

    /**
     * Returns the number of currently open data segments.
     *
     * @return open data segment count
     */
    public int getOpenDataSegments() {
        return openDataSegments;
    }

    /**
     * Returns the total number of data segments on disk.
     *
     * @return data segment count
     */
    public int getDataSegments() {
        return dataSegments;
    }

    /**
     * Returns the total number of records across all segments.
     *
     * @return total record count
     */
    public long getTotalRecordCount() {
        return totalRecordCount;
    }

    /**
     * Returns the total compressed data size in bytes.
     *
     * @return total data size
     */
    public long getTotalDataSize() {
        return totalDataSize;
    }

    /**
     * Returns the total uncompressed data size in bytes.
     *
     * @return total uncompressed size
     */
    public long getTotalUncompressedSize() {
        return totalUncompressedSize;
    }

    /**
     * Returns the total number of invalid records found during scans.
     *
     * @return invalid record count
     */
    public long getTotalInvalidRecordCount() {
        return totalInvalidRecordCount;
    }

    /**
     * Returns the minimum timestamp across all data segments, or {@code null}
     * if the dataset is empty.
     *
     * @return minimum timestamp, or {@code null}
     */
    public Long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Returns the maximum timestamp across all data segments, or {@code null}
     * if the dataset is empty.
     *
     * @return maximum timestamp, or {@code null}
     */
    public Long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Returns the number of currently open index segments.
     *
     * @return open index segment count
     */
    public int getOpenIndexSegments() {
        return openIndexSegments;
    }

    /**
     * Returns the total number of index segments on disk.
     *
     * @return index segment count
     */
    public int getIndexSegments() {
        return indexSegments;
    }

    /**
     * Returns the number of pending (not yet flushed) index entries.
     *
     * @return pending index entry count
     */
    public int getPendingIndexEntries() {
        return pendingIndexEntries;
    }

    /**
     * Returns the base timestamp of the current pending index segment,
     * or {@code null} if none.
     *
     * @return base timestamp, or {@code null}
     */
    public Long getBaseTimestamp() {
        return baseTimestamp;
    }

    /**
     * Returns whether the dataset is in read-only mode.
     *
     * @return {@code true} if read-only
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Returns whether the block cache is active for this dataset.
     *
     * @return {@code true} if block cache is present
     */
    public boolean isHasBlockCache() {
        return hasBlockCache;
    }

    /**
     * Returns whether journal is active for this dataset.
     *
     * @return {@code true} if journal is present
     */
    public boolean isHasJournal() {
        return hasJournal;
    }

    /**
     * Returns whether a queue is active for this dataset.
     *
     * @return {@code true} if queue is present
     */
    public boolean isHasQueue() {
        return hasQueue;
    }

    /**
     * Returns the number of active queue consumer groups.
     *
     * @return consumer group count
     */
    public int getQueueConsumerGroups() {
        return queueConsumerGroups;
    }
}
