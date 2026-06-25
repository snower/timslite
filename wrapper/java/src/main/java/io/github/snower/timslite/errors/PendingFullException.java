package io.github.snower.timslite.errors;

/**
 * Thrown when the pending queue is full and cannot accept more records.
 */
public class PendingFullException extends TmslException {
    /**
     * Creates a new pending-full exception.
     *
     * @param message error message
     */
    public PendingFullException(String message) {
        super(message, TmslErrorCode.PENDING_FULL);
    }
}
