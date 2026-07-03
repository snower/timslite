using System;
using System.Collections.Generic;
using Timslite.Errors;

namespace Timslite;

/// <summary>
/// Represents an open dataset queue for pushing records and opening consumers.
/// </summary>
public sealed class Queue : IDisposable
{
    private uniffi.timslite.QueueBridge? _bridge;
    private bool _disposed;

    internal Queue(uniffi.timslite.QueueBridge bridge)
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
            throw new ObjectDisposedException(nameof(Queue), "Queue is closed");
        }
    }

    /// <summary>
    /// Push a record to the queue. Returns the timestamp assigned to the record.
    /// </summary>
    public long Push(byte[] data)
    {
        CheckNotClosed();
        try
        {
            return _bridge!.Push(data);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    /// <summary>
    /// Open a consumer for the given consumer group.
    /// </summary>
    public QueueConsumer OpenConsumer(string groupName)
    {
        return OpenConsumer(groupName, new QueueConsumerOptions());
    }

    /// <summary>
    /// Open a consumer for the given consumer group with custom options.
    /// </summary>
    public QueueConsumer OpenConsumer(string groupName, QueueConsumerOptions options)
    {
        CheckNotClosed();
        try
        {
            var consumerBridge = _bridge!.OpenConsumer(groupName, options.ToNative());
            return new QueueConsumer(consumerBridge);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    /// <summary>
    /// Get the list of consumer group names registered on this queue.
    /// </summary>
    public string[] GetConsumerGroupNames()
    {
        CheckNotClosed();
        try
        {
            return _bridge!.GetConsumerGroupNames();
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    /// <summary>
    /// Drop a consumer group from this queue.
    /// </summary>
    public void DropConsumer(string groupName)
    {
        CheckNotClosed();
        try
        {
            _bridge!.DropConsumer(groupName);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }
}
