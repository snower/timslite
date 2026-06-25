package io.github.snower.timslite.errors;

/**
 * Thrown when a file has an invalid magic number (not a timslite file).
 */
public class InvalidMagicException extends TmslException {
    /**
     * Creates a new invalid magic exception.
     *
     * @param message error message
     */
    public InvalidMagicException(String message) {
        super(message, TmslErrorCode.INVALID_MAGIC);
    }
}
