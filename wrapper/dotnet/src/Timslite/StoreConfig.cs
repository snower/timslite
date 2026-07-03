namespace Timslite;

/// <summary>
/// Immutable configuration for opening a Store.
/// All properties are optional; null means use the Rust default.
/// </summary>
public sealed record StoreConfig
{
    public ulong? FlushIntervalSeconds { get; init; }
    public ulong? IdleTimeoutSeconds { get; init; }
    public ulong? DataSegmentSize { get; init; }
    public ulong? IndexSegmentSize { get; init; }
    public ulong? InitialDataSegmentSize { get; init; }
    public ulong? InitialIndexSegmentSize { get; init; }
    public byte? CompressLevel { get; init; }
    public ulong? CacheMaxMemory { get; init; }
    public ulong? CacheIdleTimeoutSeconds { get; init; }
    public byte? RetentionCheckHour { get; init; }
    public bool? EnableBackgroundThread { get; init; }
    public bool? EnableJournal { get; init; }
    public bool? ReadOnly { get; init; }

    internal uniffi.timslite.StoreConfig ToNative()
    {
        return new uniffi.timslite.StoreConfig(
            FlushIntervalSecs: FlushIntervalSeconds,
            IdleTimeoutSecs: IdleTimeoutSeconds,
            DataSegmentSize: DataSegmentSize,
            IndexSegmentSize: IndexSegmentSize,
            InitialDataSegmentSize: InitialDataSegmentSize,
            InitialIndexSegmentSize: InitialIndexSegmentSize,
            CompressLevel: CompressLevel,
            CacheMaxMemory: CacheMaxMemory,
            CacheIdleTimeoutSecs: CacheIdleTimeoutSeconds,
            RetentionCheckHour: RetentionCheckHour,
            EnableBackgroundThread: EnableBackgroundThread,
            EnableJournal: EnableJournal,
            ReadOnly: ReadOnly
        );
    }
}
