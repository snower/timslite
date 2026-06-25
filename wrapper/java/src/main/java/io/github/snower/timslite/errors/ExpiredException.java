package io.github.snower.timslite.errors;

/**
 * Thrown when a timestamp falls outside the retention window.
 */
public class ExpiredException extends TmslException {
    /**
     * Creates a new expired exception.
     *
     * @param message error message
     */
    public ExpiredException(String message) {
        super(message, TmslErrorCode.EXPIRED);
    }
}
