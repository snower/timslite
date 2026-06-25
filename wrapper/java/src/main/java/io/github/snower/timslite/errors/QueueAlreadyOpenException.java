package io.github.snower.timslite.errors;

/**
 * Thrown when attempting to open a queue that is already open.
 */
public class QueueAlreadyOpenException extends TmslException {
    /**
     * Creates a new queue-already-open exception.
     *
     * @param message error message
     */
    public QueueAlreadyOpenException(String message) {
        super(message, TmslErrorCode.QUEUE_ALREADY_OPEN);
    }
}
