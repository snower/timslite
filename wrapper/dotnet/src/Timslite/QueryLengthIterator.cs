using System;
using System.Collections;
using System.Collections.Generic;
using Timslite.Errors;

namespace Timslite;

public sealed class QueryLengthIterator : IEnumerator<LengthEntry>, IEnumerable<LengthEntry>, IDisposable
{
    private uniffi.timslite.QueryLengthIteratorBridge? _bridge;
    private LengthEntry? _current;
    private bool _exhausted;
    private bool _disposed;

    internal QueryLengthIterator(uniffi.timslite.QueryLengthIteratorBridge bridge)
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

    public LengthEntry Current => _current ?? throw new InvalidOperationException("No current entry");

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
            _current = new LengthEntry(result.Timestamp, result.Length);
            return true;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void Reset()
    {
        throw new NotSupportedException("QueryLengthIterator does not support Reset");
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

    public IReadOnlyList<LengthEntry> CollectAll()
    {
        CheckNotDisposed();
        try
        {
            var results = _bridge!.CollectAll();
            _exhausted = true;
            var entries = new LengthEntry[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                entries[i] = new LengthEntry(results[i].Timestamp, results[i].Length);
            }
            return entries;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public IReadOnlyList<LengthEntry> CollectTake(uint count)
    {
        CheckNotDisposed();
        try
        {
            var results = _bridge!.CollectTake(count);
            var entries = new LengthEntry[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                entries[i] = new LengthEntry(results[i].Timestamp, results[i].Length);
            }
            return entries;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public IEnumerator<LengthEntry> GetEnumerator() => this;

    IEnumerator IEnumerable.GetEnumerator() => this;

    private void CheckNotDisposed()
    {
        if (_disposed || _bridge is null)
        {
            throw new ObjectDisposedException(nameof(QueryLengthIterator));
        }
    }
}
