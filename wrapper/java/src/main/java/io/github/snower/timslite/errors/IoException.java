package io.github.snower.timslite.errors;

/**
 * Thrown when an I/O error occurs (disk read/write failure, etc.).
 */
public class IoException extends TmslException {
    /**
     * Creates a new I/O exception.
     *
     * @param message error message
     */
    public IoException(String message) {
        super(message, TmslErrorCode.IO);
    }
}
