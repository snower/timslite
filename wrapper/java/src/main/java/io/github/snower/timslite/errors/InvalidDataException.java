package io.github.snower.timslite.errors;

/**
 * Thrown when data is invalid or corrupt.
 */
public class InvalidDataException extends TmslException {
    /**
     * Creates a new invalid data exception.
     *
     * @param message error message
     */
    public InvalidDataException(String message) {
        super(message, TmslErrorCode.INVALID_DATA);
    }
}
