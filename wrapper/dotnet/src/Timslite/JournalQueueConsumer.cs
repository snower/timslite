using System;
using System.Threading;
using System.Threading.Tasks;
using Timslite.Errors;

namespace Timslite;

/// <summary>
/// A journal queue consumer that can poll for journal records and acknowledge processing.
/// </summary>
public sealed class JournalQueueConsumer : IDisposable
{
    private uniffi.timslite.JournalQueueConsumerBridge? _bridge;
    private bool _disposed;

    internal JournalQueueConsumer(uniffi.timslite.JournalQueueConsumerBridge bridge)
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
            throw new ObjectDisposedException(nameof(JournalQueueConsumer), "Journal queue consumer is closed");
        }
    }

    /// <summary>
    /// Poll for the next journal record, blocking up to the specified timeout.
    /// Returns null if no record is available within the timeout.
    /// </summary>
    public JournalRecord? Poll(TimeSpan timeout)
    {
        CheckNotClosed();
        try
        {
            var result = _bridge!.Poll((ulong)(long)timeout.TotalMilliseconds);
            return result is null ? null : ConvertJournalRecord(result);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    /// <summary>
    /// Asynchronously poll for the next journal record, blocking up to the specified timeout.
    /// Returns null if no record is available within the timeout.
    /// </summary>
    public Task<JournalRecord?> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        return Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Poll(timeout);
        }, cancellationToken);
    }

    /// <summary>
    /// Acknowledge that a journal record has been processed.
    /// </summary>
    public void Ack(long sequence)
    {
        CheckNotClosed();
        try
        {
            _bridge!.Ack(sequence);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    private static JournalRecord ConvertJournalRecord(uniffi.timslite.JournalRecord r)
    {
        return new JournalRecord(r.Sequence, r.Data);
    }
}
