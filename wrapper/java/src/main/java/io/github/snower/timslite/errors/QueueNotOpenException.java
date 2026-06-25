package io.github.snower.timslite.errors;

/**
 * Thrown when attempting to use a queue that is not open.
 */
public class QueueNotOpenException extends TmslException {
    /**
     * Creates a new queue-not-open exception.
     *
     * @param message error message
     */
    public QueueNotOpenException(String message) {
        super(message, TmslErrorCode.QUEUE_NOT_OPEN);
    }
}
