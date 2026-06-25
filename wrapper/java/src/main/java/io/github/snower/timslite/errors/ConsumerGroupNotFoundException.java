package io.github.snower.timslite.errors;

/**
 * Thrown when a consumer group is not found.
 */
public class ConsumerGroupNotFoundException extends TmslException {
    /**
     * Creates a new consumer-group-not-found exception.
     *
     * @param message error message
     */
    public ConsumerGroupNotFoundException(String message) {
        super(message, TmslErrorCode.CONSUMER_GROUP_NOT_FOUND);
    }
}
