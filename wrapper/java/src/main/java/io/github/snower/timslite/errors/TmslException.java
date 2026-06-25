package io.github.snower.timslite.errors;

/**
 * Base exception for all timslite errors.
 *
 * <p>Every error thrown by the timslite Java wrapper is a subclass of this
 * exception. Use {@link #code()} to inspect the specific error category.</p>
 *
 * <p>Example:</p>
 * <pre>{@code
 * try {
 *     ds.read(1700000000L);
 * } catch (TmslException e) {
 *     if (e.code() == TmslErrorCode.EXPIRED) {
 *         // timestamp outside retention window
 *     }
 * }
 * }</pre>
 */
public class TmslException extends RuntimeException {
    private final TmslErrorCode code;

    /**
     * Creates a new exception with the given message and error code.
     *
     * @param message human-readable error message
     * @param code    error category
     */
    public TmslException(String message, TmslErrorCode code) {
        super(message);
        this.code = code;
    }

    /**
     * Returns the error code for this exception.
     *
     * @return the error code
     */
    public TmslErrorCode code() {
        return code;
    }

    /**
     * Converts a UniFFI-generated exception to the corresponding Java exception.
     *
     * @param e the UniFFI exception
     * @return the Java exception subclass
     */
    @SuppressWarnings("deprecation")
    public static TmslException fromUniFFI(io.github.snower.timslite.uniffi.TmslException e) {
        String msg = e.getMessage();
        if (e instanceof io.github.snower.timslite.uniffi.TmslException.Io) {
            return new IoException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.InvalidMagic) {
            return new InvalidMagicException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.InvalidVersion) {
            return new InvalidVersionException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.MmapException) {
            return new MmapException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.CompressionException) {
            return new CompressionException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.DecompressionException) {
            return new DecompressionException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.InvalidData) {
            return new InvalidDataException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.NotFound) {
            return new NotFoundException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.Expired) {
            return new ExpiredException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.AlreadyExists) {
            return new AlreadyExistsException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.SegmentFull) {
            return new SegmentFullException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.QueueAlreadyOpen) {
            return new QueueAlreadyOpenException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.QueueNotOpen) {
            return new QueueNotOpenException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.ConsumerGroupNotFound) {
            return new ConsumerGroupNotFoundException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.ConsumerGroupExists) {
            return new ConsumerGroupExistsException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.QueueClosed) {
            return new QueueClosedException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.PendingFull) {
            return new PendingFullException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.StoreClosed) {
            return new StoreClosedException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.DatasetClosed) {
            return new DatasetClosedException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.QueueBridgeClosed) {
            return new QueueBridgeClosedException(msg);
        } else if (e instanceof io.github.snower.timslite.uniffi.TmslException.IteratorExhausted) {
            return new IteratorExhaustedException(msg);
        }
        return new TmslException(msg, TmslErrorCode.IO);
    }
}
