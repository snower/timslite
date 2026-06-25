package io.github.snower.timslite.errors;

/**
 * Thrown when attempting to create a resource that already exists.
 */
public class AlreadyExistsException extends TmslException {
    /**
     * Creates a new already-exists exception.
     *
     * @param message error message
     */
    public AlreadyExistsException(String message) {
        super(message, TmslErrorCode.ALREADY_EXISTS);
    }
}
