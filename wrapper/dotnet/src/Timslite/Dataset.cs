using System;
using System.Collections.Generic;
using Timslite.Errors;

namespace Timslite;

public sealed class Dataset : IDisposable
{
    private uniffi.timslite.DatasetBridge? _bridge;
    private bool _disposed;

    internal Dataset(uniffi.timslite.DatasetBridge bridge)
    {
        _bridge = bridge;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _bridge?.Destroy();
            _bridge = null;
        }
        GC.SuppressFinalize(this);
    }

    public bool IsClosed => _disposed || (_bridge?.IsClosed() ?? true);

    public ulong Identifier
    {
        get
        {
            CheckNotClosed();
            return _bridge!.Identifier();
        }
    }

    internal uniffi.timslite.DatasetBridge Bridge
    {
        get
        {
            CheckNotClosed();
            return _bridge!;
        }
    }

    private void CheckNotClosed()
    {
        if (_disposed || _bridge is null)
        {
            throw new ObjectDisposedException(nameof(Dataset), "Dataset is closed");
        }
    }

    public void Write(long timestamp, byte[] data)
    {
        CheckNotClosed();
        try
        {
            _bridge!.Write(timestamp, data);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void WriteNow(byte[] data)
    {
        CheckNotClosed();
        try
        {
            _bridge!.WriteNow(data);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void Append(long timestamp, byte[] data)
    {
        CheckNotClosed();
        try
        {
            _bridge!.Append(timestamp, data);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void AppendNow(byte[] data)
    {
        CheckNotClosed();
        try
        {
            _bridge!.AppendNow(data);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void Delete(long timestamp)
    {
        CheckNotClosed();
        try
        {
            _bridge!.Delete(timestamp);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

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

    public Record? Read(long timestamp)
    {
        CheckNotClosed();
        try
        {
            var result = _bridge!.Read(timestamp);
            return result is null ? null : ConvertRecord(result);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public Record? ReadLatest()
    {
        CheckNotClosed();
        try
        {
            var result = _bridge!.ReadLatest();
            return result is null ? null : ConvertRecord(result);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public bool ReadExist(long timestamp)
    {
        CheckNotClosed();
        try
        {
            return _bridge!.ReadExist(timestamp);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public uint? ReadLength(long timestamp)
    {
        CheckNotClosed();
        try
        {
            return _bridge!.ReadLength(timestamp);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public IReadOnlyList<Record> Query(long startTs, long endTs)
    {
        CheckNotClosed();
        try
        {
            var results = _bridge!.Query(startTs, endTs);
            var records = new Record[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                records[i] = ConvertRecord(results[i]);
            }
            return records;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public byte[] QueryExist(long startTs, long endTs)
    {
        CheckNotClosed();
        try
        {
            return _bridge!.QueryExist(startTs, endTs);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public IReadOnlyList<LengthEntry> QueryLength(long startTs, long endTs)
    {
        CheckNotClosed();
        try
        {
            var results = _bridge!.QueryLength(startTs, endTs);
            var entries = new LengthEntry[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                var r = results[i];
                entries[i] = new LengthEntry(r.Timestamp, r.Length);
            }
            return entries;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public QueryIterator QueryIter(long startTs, long endTs)
    {
        CheckNotClosed();
        try
        {
            var bridge = _bridge!.QueryIter(startTs, endTs);
            return new QueryIterator(bridge);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public QueryLengthIterator QueryLengthIter(long startTs, long endTs)
    {
        CheckNotClosed();
        try
        {
            var bridge = _bridge!.QueryLengthIter(startTs, endTs);
            return new QueryLengthIterator(bridge);
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
}
