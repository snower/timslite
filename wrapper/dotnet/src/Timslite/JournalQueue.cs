using System;
using Timslite.Errors;

namespace Timslite;

/// <summary>
/// Represents an open journal queue for consuming journal records.
/// </summary>
public sealed class JournalQueue : IDisposable
{
    private uniffi.timslite.JournalQueueBridge? _bridge;
    private bool _disposed;

    internal JournalQueue(uniffi.timslite.JournalQueueBridge bridge)
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
            throw new ObjectDisposedException(nameof(JournalQueue), "Journal queue is closed");
        }
    }

    /// <summary>
    /// Open a consumer for the given consumer group.
    /// </summary>
    public JournalQueueConsumer OpenConsumer(string groupName)
    {
        return OpenConsumer(groupName, new QueueConsumerOptions());
    }

    /// <summary>
    /// Open a consumer for the given consumer group with custom options.
    /// </summary>
    public JournalQueueConsumer OpenConsumer(string groupName, QueueConsumerOptions options)
    {
        CheckNotClosed();
        try
        {
            var consumerBridge = _bridge!.OpenConsumer(groupName, options.ToNative());
            return new JournalQueueConsumer(consumerBridge);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }
}
