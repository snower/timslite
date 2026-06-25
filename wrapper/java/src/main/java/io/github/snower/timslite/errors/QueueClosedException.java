package io.github.snower.timslite.errors;

/**
 * Thrown when attempting to use a queue that has been closed.
 */
public class QueueClosedException extends TmslException {
    /**
     * Creates a new queue-closed exception.
     *
     * @param message error message
     */
    public QueueClosedException(String message) {
        super(message, TmslErrorCode.QUEUE_CLOSED);
    }
}
