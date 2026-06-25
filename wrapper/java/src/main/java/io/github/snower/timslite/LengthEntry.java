package io.github.snower.timslite;

/**
 * A timestamp and record-length pair returned by query length operations.
 */
public final class LengthEntry {
    private final long timestamp;
    private final int length;

    LengthEntry(io.github.snower.timslite.uniffi.LengthEntry kotlinEntry) {
        this.timestamp = kotlinEntry.getTimestamp();
        this.length = KotlinConversions.getUInt(kotlinEntry, "getLength");
    }

    /**
     * Returns the timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the record length in bytes.
     *
     * @return the length
     */
    public int getLength() {
        return length;
    }
}
