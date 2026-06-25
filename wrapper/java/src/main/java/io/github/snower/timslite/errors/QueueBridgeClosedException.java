package io.github.snower.timslite.errors;

/**
 * Thrown when the queue bridge connection is closed.
 */
public class QueueBridgeClosedException extends TmslException {
    /**
     * Creates a new queue-bridge-closed exception.
     *
     * @param message error message
     */
    public QueueBridgeClosedException(String message) {
        super(message, TmslErrorCode.QUEUE_BRIDGE_CLOSED);
    }
}
