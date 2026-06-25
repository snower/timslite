package io.github.snower.timslite.errors;

/**
 * Thrown when data compression fails.
 */
public class CompressionException extends TmslException {
    /**
     * Creates a new compression exception.
     *
     * @param message error message
     */
    public CompressionException(String message) {
        super(message, TmslErrorCode.COMPRESSION);
    }
}
