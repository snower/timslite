package io.github.snower.timslite;

/**
 * A time-series record containing a timestamp and payload data.
 *
 * <p>Timestamps are signed 64-bit values ({@code long}). The data payload
 * is returned as a defensive copy.</p>
 */
public final class Record {
    private final long timestamp;
    private final byte[] data;

    Record(io.github.snower.timslite.uniffi.Record kotlinRecord) {
        this.timestamp = kotlinRecord.getTimestamp();
        this.data = KotlinConversions.fromUByteList(kotlinRecord.getData());
    }

    /**
     * Returns the timestamp of this record.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns a defensive copy of the record payload.
     *
     * @return the payload bytes
     */
    public byte[] getData() {
        return data.clone();
    }
}
