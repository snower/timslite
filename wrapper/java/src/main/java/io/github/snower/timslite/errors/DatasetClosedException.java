package io.github.snower.timslite.errors;

/**
 * Thrown when attempting to use a dataset that has been closed.
 */
public class DatasetClosedException extends TmslException {
    /**
     * Creates a new dataset-closed exception.
     *
     * @param message error message
     */
    public DatasetClosedException(String message) {
        super(message, TmslErrorCode.DATASET_CLOSED);
    }
}
