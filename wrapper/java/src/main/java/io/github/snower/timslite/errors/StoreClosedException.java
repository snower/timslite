package io.github.snower.timslite.errors;

/**
 * Thrown when attempting to use a store that has been closed.
 */
public class StoreClosedException extends TmslException {
    /**
     * Creates a new store-closed exception.
     *
     * @param message error message
     */
    public StoreClosedException(String message) {
        super(message, TmslErrorCode.STORE_CLOSED);
    }
}
