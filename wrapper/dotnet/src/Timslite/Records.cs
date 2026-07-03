using System.Collections.Generic;

namespace Timslite;

public sealed record DataSetInfo(
    string Name,
    string DatasetType,
    string BaseDir,
    ulong Identifier,
    ulong DataSegmentSize,
    ulong IndexSegmentSize,
    ulong InitialDataSegmentSize,
    ulong InitialIndexSegmentSize,
    byte CompressType,
    byte CompressLevel,
    byte IndexContinuous,
    ulong RetentionWindow,
    bool EnableJournal,
    long CreateTime
);

public sealed record DataSetState(
    long? LatestWrittenTimestamp,
    uint OpenDataSegments,
    uint DataSegments,
    ulong TotalRecordCount,
    ulong TotalDataSize,
    ulong TotalUncompressedSize,
    ulong TotalInvalidRecordCount,
    long? MinTimestamp,
    long? MaxTimestamp,
    uint OpenIndexSegments,
    uint IndexSegments,
    uint PendingIndexEntries,
    long? BaseTimestamp,
    bool ReadOnly,
    bool HasBlockCache,
    bool HasJournal,
    bool HasQueue,
    uint QueueConsumerGroups
);

public sealed record DataSetInspectResult(DataSetInfo Info, DataSetState State);

public sealed record QueueConsumerInfo(
    string GroupName,
    ulong RunningExpiredSeconds,
    ushort MaxRetryCount
);

public sealed record QueueConsumerPendingEntry(
    long Timestamp,
    long StartTime,
    byte Status,
    byte RetryCount
);

public sealed record QueueConsumerState(
    long ProcessedTs,
    IReadOnlyList<QueueConsumerPendingEntry> PendingEntries
);

public sealed record QueueConsumerInspectResult(QueueConsumerInfo Info, QueueConsumerState State);

public sealed record Record(long Timestamp, byte[] Data)
{
    public byte[] Data { get; init; } = (byte[])Data.Clone();
}

public sealed record JournalRecord(long Sequence, byte[] Data)
{
    public byte[] Data { get; init; } = (byte[])Data.Clone();
}

public sealed record LengthEntry(long Timestamp, uint Length);

public sealed record TickResult(ulong ExecutedTasks, ulong NextDelayMs);
