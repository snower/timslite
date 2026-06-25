package io.github.snower.timslite.errors;

/**
 * Thrown when a requested record or resource is not found.
 */
public class NotFoundException extends TmslException {
    /**
     * Creates a new not-found exception.
     *
     * @param message error message
     */
    public NotFoundException(String message) {
        super(message, TmslErrorCode.NOT_FOUND);
    }
}
