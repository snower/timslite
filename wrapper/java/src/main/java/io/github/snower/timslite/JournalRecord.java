package io.github.snower.timslite;

/**
 * A journal change-log record containing a sequence number and payload data.
 *
 * <p>Journal sequences start at 1 and increase monotonically. The data
 * payload is returned as a defensive copy.</p>
 */
public final class JournalRecord {
    private final long sequence;
    private final byte[] data;

    JournalRecord(io.github.snower.timslite.uniffi.JournalRecord kotlinRecord) {
        this.sequence = kotlinRecord.getSequence();
        this.data = KotlinConversions.fromUByteList(kotlinRecord.getData());
    }

    /**
     * Returns the journal sequence number.
     *
     * @return the sequence number
     */
    public long getSequence() {
        return sequence;
    }

    /**
     * Returns a defensive copy of the journal record payload.
     *
     * @return the payload bytes
     */
    public byte[] getData() {
        return data.clone();
    }
}
