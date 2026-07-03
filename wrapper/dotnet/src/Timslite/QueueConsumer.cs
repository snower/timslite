using System;
using System.Threading;
using System.Threading.Tasks;
using Timslite.Errors;

namespace Timslite;

/// <summary>
/// A queue consumer that can poll for records and acknowledge processing.
/// </summary>
public sealed class QueueConsumer : IDisposable
{
    private uniffi.timslite.QueueConsumerBridge? _bridge;
    private bool _disposed;

    internal QueueConsumer(uniffi.timslite.QueueConsumerBridge bridge)
    {
        _bridge = bridge;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            try { _bridge?.Release(); } catch { }
            _bridge?.Destroy();
            _bridge = null;
        }
        GC.SuppressFinalize(this);
    }

    public bool IsClosed => _disposed || (_bridge is null);

    private void CheckNotClosed()
    {
        if (_disposed || _bridge is null)
        {
            throw new ObjectDisposedException(nameof(QueueConsumer), "Queue consumer is closed");
        }
    }

    /// <summary>
    /// Poll for the next record, blocking up to the specified timeout.
    /// Returns null if no record is available within the timeout.
    /// </summary>
    public Record? Poll(TimeSpan timeout)
    {
        CheckNotClosed();
        try
        {
            var result = _bridge!.Poll((ulong)(long)timeout.TotalMilliseconds);
            return result is null ? null : ConvertRecord(result);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    /// <summary>
    /// Asynchronously poll for the next record, blocking up to the specified timeout.
    /// Returns null if no record is available within the timeout.
    /// </summary>
    public Task<Record?> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        return Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Poll(timeout);
        }, cancellationToken);
    }

    /// <summary>
    /// Acknowledge that a record has been processed.
    /// </summary>
    public void Ack(long timestamp)
    {
        CheckNotClosed();
        try
        {
            _bridge!.Ack(timestamp);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    /// <summary>
    /// Flush pending consumer state to disk.
    /// </summary>
    public void Flush()
    {
        CheckNotClosed();
        try
        {
            _bridge!.Flush();
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    /// <summary>
    /// Inspect the consumer's info and state.
    /// </summary>
    public QueueConsumerInspectResult Inspect()
    {
        CheckNotClosed();
        try
        {
            var result = _bridge!.Inspect();
            return ConvertInspectResult(result);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    private static Record ConvertRecord(uniffi.timslite.Record r)
    {
        return new Record(r.Timestamp, r.Data);
    }

    private static QueueConsumerInspectResult ConvertInspectResult(uniffi.timslite.QueueConsumerInspectResult r)
    {
        var info = new QueueConsumerInfo(
            r.Info.GroupName,
            r.Info.RunningExpiredSeconds,
            r.Info.MaxRetryCount
        );
        var pendingEntries = new QueueConsumerPendingEntry[r.State.PendingEntries.Length];
        for (int i = 0; i < r.State.PendingEntries.Length; i++)
        {
            var entry = r.State.PendingEntries[i];
            pendingEntries[i] = new QueueConsumerPendingEntry(
                entry.Timestamp,
                entry.StartTime,
                entry.Status,
                entry.RetryCount
            );
        }
        var state = new QueueConsumerState(
            r.State.ProcessedTs,
            pendingEntries
        );
        return new QueueConsumerInspectResult(info, state);
    }
}
