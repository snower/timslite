using System;

namespace Timslite.Errors;

public class TmslException : Exception
{
    public TmslErrorCode Code { get; }

    public TmslException(TmslErrorCode code, string message) : base(message)
    {
        Code = code;
    }

    public TmslException(TmslErrorCode code, string message, Exception innerException)
        : base(message, innerException)
    {
        Code = code;
    }

    internal static TmslException FromUniFFI(uniffi.timslite.TmslException ex)
    {
        return ex switch
        {
            uniffi.timslite.TmslException.Io e =>
                new TmslException(TmslErrorCode.Io, e.Message),
            uniffi.timslite.TmslException.InvalidMagic e =>
                new TmslException(TmslErrorCode.InvalidMagic, e.Message),
            uniffi.timslite.TmslException.InvalidVersion e =>
                new TmslException(TmslErrorCode.InvalidVersion, e.Message),
            uniffi.timslite.TmslException.MmapException e =>
                new TmslException(TmslErrorCode.MmapError, e.Message),
            uniffi.timslite.TmslException.CompressionException e =>
                new TmslException(TmslErrorCode.CompressionError, e.Message),
            uniffi.timslite.TmslException.DecompressionException e =>
                new TmslException(TmslErrorCode.DecompressionError, e.Message),
            uniffi.timslite.TmslException.InvalidData e =>
                new TmslException(TmslErrorCode.InvalidData, e.Message),
            uniffi.timslite.TmslException.NotFound e =>
                new TmslException(TmslErrorCode.NotFound, e.Message),
            uniffi.timslite.TmslException.Expired e =>
                new TmslException(TmslErrorCode.Expired, e.Message),
            uniffi.timslite.TmslException.AlreadyExists e =>
                new TmslException(TmslErrorCode.AlreadyExists, e.Message),
            uniffi.timslite.TmslException.SegmentFull e =>
                new TmslException(TmslErrorCode.SegmentFull, e.Message),
            uniffi.timslite.TmslException.QueueAlreadyOpen e =>
                new TmslException(TmslErrorCode.QueueAlreadyOpen, e.Message),
            uniffi.timslite.TmslException.QueueNotOpen e =>
                new TmslException(TmslErrorCode.QueueNotOpen, e.Message),
            uniffi.timslite.TmslException.ConsumerGroupNotFound e =>
                new TmslException(TmslErrorCode.ConsumerGroupNotFound, e.Message),
            uniffi.timslite.TmslException.ConsumerGroupExists e =>
                new TmslException(TmslErrorCode.ConsumerGroupExists, e.Message),
            uniffi.timslite.TmslException.QueueClosed e =>
                new TmslException(TmslErrorCode.QueueClosed, e.Message),
            uniffi.timslite.TmslException.PendingFull e =>
                new TmslException(TmslErrorCode.PendingFull, e.Message),
            uniffi.timslite.TmslException.StoreClosed e =>
                new TmslException(TmslErrorCode.StoreClosed, e.Message),
            uniffi.timslite.TmslException.DatasetClosed e =>
                new TmslException(TmslErrorCode.DatasetClosed, e.Message),
            uniffi.timslite.TmslException.QueueBridgeClosed e =>
                new TmslException(TmslErrorCode.QueueBridgeClosed, e.Message),
            uniffi.timslite.TmslException.IteratorExhausted e =>
                new TmslException(TmslErrorCode.IteratorExhausted, e.Message),
            _ => new TmslException(TmslErrorCode.Io, ex.Message),
        };
    }
}
