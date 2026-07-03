namespace Timslite.Errors;

public enum TmslErrorCode
{
    Io,
    InvalidMagic,
    InvalidVersion,
    MmapError,
    CompressionError,
    DecompressionError,
    InvalidData,
    NotFound,
    Expired,
    AlreadyExists,
    SegmentFull,
    QueueAlreadyOpen,
    QueueNotOpen,
    ConsumerGroupNotFound,
    ConsumerGroupExists,
    QueueClosed,
    PendingFull,
    StoreClosed,
    DatasetClosed,
    QueueBridgeClosed,
    IteratorExhausted,
}
