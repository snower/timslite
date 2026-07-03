namespace Timslite;

/// <summary>
/// Immutable per-dataset configuration overrides.
/// </summary>
public sealed record DatasetConfig
{
    public ulong? DataSegmentSize { get; init; }
    public ulong? IndexSegmentSize { get; init; }
    public ulong? InitialDataSegmentSize { get; init; }
    public ulong? InitialIndexSegmentSize { get; init; }
    public byte? CompressLevel { get; init; }
    public byte? CompressType { get; init; }
    public byte? IndexContinuous { get; init; }
    public ulong? RetentionWindow { get; init; }
    public bool? EnableJournal { get; init; }

    internal uniffi.timslite.DatasetConfig ToNative()
    {
        return new uniffi.timslite.DatasetConfig(
            DataSegmentSize: DataSegmentSize,
            IndexSegmentSize: IndexSegmentSize,
            InitialDataSegmentSize: InitialDataSegmentSize,
            InitialIndexSegmentSize: InitialIndexSegmentSize,
            CompressLevel: CompressLevel,
            CompressType: CompressType,
            IndexContinuous: IndexContinuous,
            RetentionWindow: RetentionWindow,
            EnableJournal: EnableJournal
        );
    }
}
