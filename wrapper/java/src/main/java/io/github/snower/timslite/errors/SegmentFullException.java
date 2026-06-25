package io.github.snower.timslite.errors;

/**
 * Thrown when a data segment is full and cannot accept more records.
 */
public class SegmentFullException extends TmslException {
    /**
     * Creates a new segment-full exception.
     *
     * @param message error message
     */
    public SegmentFullException(String message) {
        super(message, TmslErrorCode.SEGMENT_FULL);
    }
}
