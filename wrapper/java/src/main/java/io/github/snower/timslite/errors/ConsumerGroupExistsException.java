package io.github.snower.timslite.errors;

/**
 * Thrown when attempting to create a consumer group that already exists.
 */
public class ConsumerGroupExistsException extends TmslException {
    /**
     * Creates a new consumer-group-exists exception.
     *
     * @param message error message
     */
    public ConsumerGroupExistsException(String message) {
        super(message, TmslErrorCode.CONSUMER_GROUP_EXISTS);
    }
}
