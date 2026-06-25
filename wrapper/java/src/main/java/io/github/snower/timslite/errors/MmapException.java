package io.github.snower.timslite.errors;

/**
 * Thrown when a memory-mapping operation fails.
 */
public class MmapException extends TmslException {
    /**
     * Creates a new mmap exception.
     *
     * @param message error message
     */
    public MmapException(String message) {
        super(message, TmslErrorCode.MMAP);
    }
}
