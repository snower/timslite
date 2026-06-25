package io.github.snower.timslite.errors;

/**
 * Thrown when data decompression fails.
 */
public class DecompressionException extends TmslException {
    /**
     * Creates a new decompression exception.
     *
     * @param message error message
     */
    public DecompressionException(String message) {
        super(message, TmslErrorCode.DECOMPRESSION);
    }
}
