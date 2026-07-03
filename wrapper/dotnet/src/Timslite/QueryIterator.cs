using System;
using System.Collections;
using System.Collections.Generic;
using Timslite.Errors;

namespace Timslite;

public sealed class QueryIterator : IEnumerator<Record>, IEnumerable<Record>, IDisposable
{
    private uniffi.timslite.QueryIteratorBridge? _bridge;
    private Record? _current;
    private bool _exhausted;
    private bool _disposed;

    internal QueryIterator(uniffi.timslite.QueryIteratorBridge bridge)
    {
        _bridge = bridge;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            try { _bridge?.Destroy(); } catch { }
            _bridge = null;
        }
    }

    public Record Current => _current ?? throw new InvalidOperationException("No current record");

    object IEnumerator.Current => Current;

    public bool MoveNext()
    {
        if (_disposed || _exhausted) return false;
        try
        {
            var result = _bridge!.Next();
            if (result is null)
            {
                _exhausted = true;
                _current = null;
                return false;
            }
            _current = new Record(result.Timestamp, result.Data);
            return true;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void Reset()
    {
        throw new NotSupportedException("QueryIterator does not support Reset");
    }

    public void Reverse()
    {
        CheckNotDisposed();
        try
        {
            _bridge!.Reverse();
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void Skip(uint count)
    {
        CheckNotDisposed();
        try
        {
            _bridge!.Skip(count);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public IReadOnlyList<Record> CollectAll()
    {
        CheckNotDisposed();
        try
        {
            var results = _bridge!.CollectAll();
            _exhausted = true;
            var records = new Record[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                records[i] = new Record(results[i].Timestamp, results[i].Data);
            }
            return records;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public IReadOnlyList<Record> CollectTake(uint count)
    {
        CheckNotDisposed();
        try
        {
            var results = _bridge!.CollectTake(count);
            var records = new Record[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                records[i] = new Record(results[i].Timestamp, results[i].Data);
            }
            return records;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public IEnumerator<Record> GetEnumerator() => this;

    IEnumerator IEnumerable.GetEnumerator() => this;

    private void CheckNotDisposed()
    {
        if (_disposed || _bridge is null)
        {
            throw new ObjectDisposedException(nameof(QueryIterator));
        }
    }
}
