namespace Timslite;

/// <summary>
/// Configuration for a queue consumer group.
/// </summary>
public sealed record QueueConsumerConfig
{
    public ulong? RunningExpiredSeconds { get; init; }
    public ushort? MaxRetryCount { get; init; }

    internal uniffi.timslite.QueueConsumerConfig ToNative()
    {
        return new uniffi.timslite.QueueConsumerConfig(
            RunningExpiredSeconds: RunningExpiredSeconds,
            MaxRetryCount: MaxRetryCount
        );
    }
}

/// <summary>
/// Options for opening a queue consumer, optionally with custom config.
/// </summary>
public sealed record QueueConsumerOptions
{
    public QueueConsumerConfig? Config { get; init; }

    internal uniffi.timslite.QueueConsumerOptions ToNative()
    {
        return new uniffi.timslite.QueueConsumerOptions(
            Config: Config?.ToNative()
        );
    }
}
