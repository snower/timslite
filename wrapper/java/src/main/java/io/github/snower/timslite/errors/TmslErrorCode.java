package io.github.snower.timslite.errors;

/**
 * Error codes corresponding to UniFFI TmslException variants.
 *
 * <p>Use with {@link TmslException#code()} to identify the specific error
 * category without catching individual exception subclasses.</p>
 */
public enum TmslErrorCode {
    /** I/O error (disk, network, etc.). */
    IO,
    /** Invalid file magic number. */
    INVALID_MAGIC,
    /** Unsupported file format version. */
    INVALID_VERSION,
    /** Memory-mapping error. */
    MMAP,
    /** Compression error. */
    COMPRESSION,
    /** Decompression error. */
    DECOMPRESSION,
    /** Invalid or corrupt data. */
    INVALID_DATA,
    /** Record or dataset not found. */
    NOT_FOUND,
    /** Timestamp falls outside the retention window. */
    EXPIRED,
    /** Dataset or consumer group already exists. */
    ALREADY_EXISTS,
    /** Data segment is full. */
    SEGMENT_FULL,
    /** Queue is already open on this dataset. */
    QUEUE_ALREADY_OPEN,
    /** Queue is not open. */
    QUEUE_NOT_OPEN,
    /** Consumer group not found. */
    CONSUMER_GROUP_NOT_FOUND,
    /** Consumer group already exists. */
    CONSUMER_GROUP_EXISTS,
    /** Queue is closed. */
    QUEUE_CLOSED,
    /** Pending queue is full. */
    PENDING_FULL,
    /** Store is closed. */
    STORE_CLOSED,
    /** Dataset is closed. */
    DATASET_CLOSED,
    /** Queue bridge is closed. */
    QUEUE_BRIDGE_CLOSED,
    /** Iterator has no more elements. */
    ITERATOR_EXHAUSTED
}
