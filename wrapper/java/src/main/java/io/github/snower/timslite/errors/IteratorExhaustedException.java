package io.github.snower.timslite.errors;

/**
 * Thrown when attempting to read from an iterator that has no more elements.
 */
public class IteratorExhaustedException extends TmslException {
    /**
     * Creates a new iterator-exhausted exception.
     *
     * @param message error message
     */
    public IteratorExhaustedException(String message) {
        super(message, TmslErrorCode.ITERATOR_EXHAUSTED);
    }
}
