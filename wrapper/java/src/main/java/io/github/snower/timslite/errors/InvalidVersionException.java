package io.github.snower.timslite.errors;

/**
 * Thrown when a file has an unsupported format version.
 */
public class InvalidVersionException extends TmslException {
    /**
     * Creates a new invalid version exception.
     *
     * @param message error message
     */
    public InvalidVersionException(String message) {
        super(message, TmslErrorCode.INVALID_VERSION);
    }
}
