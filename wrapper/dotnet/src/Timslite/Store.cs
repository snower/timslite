using System;
using System.Collections.Generic;
using Timslite.Errors;

namespace Timslite;

public sealed class Store : IDisposable
{
    private uniffi.timslite.StoreBridge? _bridge;
    private bool _disposed;

    private Store(uniffi.timslite.StoreBridge bridge)
    {
        _bridge = bridge;
    }

    public static Store Open(string path)
    {
        return Open(path, new StoreConfig());
    }

    public static Store Open(string path, StoreConfig config)
    {
        NativeLibraryLoader.Load();
        var bridge = uniffi.timslite.StoreBridge.Open(path, config.ToNative());
        return new Store(bridge);
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

    public bool IsReadOnly()
    {
        CheckNotClosed();
        try
        {
            return _bridge!.IsReadOnly();
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void CreateDataset(string name, string datasetType)
    {
        CreateDataset(name, datasetType, new CreateDatasetOptions());
    }

    public void CreateDataset(string name, string datasetType, CreateDatasetOptions options)
    {
        CheckNotClosed();
        try
        {
            _bridge!.CreateDataset(name, datasetType, options.ToNative());
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public Dataset OpenDataset(string name, string datasetType)
    {
        CheckNotClosed();
        try
        {
            var datasetBridge = _bridge!.OpenDataset(name, datasetType);
            return new Dataset(datasetBridge);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public Dataset OpenDatasetByIdentifier(ulong identifier)
    {
        CheckNotClosed();
        try
        {
            var datasetBridge = _bridge!.OpenDatasetByIdentifier(identifier);
            return new Dataset(datasetBridge);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public void DropDataset(string name, string datasetType)
    {
        CheckNotClosed();
        try
        {
            _bridge!.DropDataset(name, datasetType);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public string[] GetDatasetNames()
    {
        CheckNotClosed();
        try
        {
            return _bridge!.GetDatasetNames();
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public string[] GetDatasetTypes(string name)
    {
        CheckNotClosed();
        try
        {
            return _bridge!.GetDatasetTypes(name);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public DataSetInspectResult InspectDataset(string name, string datasetType)
    {
        CheckNotClosed();
        try
        {
            var result = _bridge!.InspectDataset(name, datasetType);
            return ConvertInspectResult(result);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public TickResult TickBackgroundTasks()
    {
        CheckNotClosed();
        try
        {
            var result = _bridge!.TickBackgroundTasks();
            return new TickResult(result.ExecutedTasks, result.NextDelayMs);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public ulong NextBackgroundDelayMs()
    {
        CheckNotClosed();
        try
        {
            return _bridge!.NextBackgroundDelayMs();
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public Queue OpenQueue(Dataset dataset)
    {
        CheckNotClosed();
        try
        {
            var queueBridge = _bridge!.OpenQueue(dataset.Bridge);
            return new Queue(queueBridge);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public JournalQueue OpenJournalQueue()
    {
        CheckNotClosed();
        try
        {
            var queueBridge = _bridge!.OpenJournalQueue();
            return new JournalQueue(queueBridge);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public long? JournalLatestSequence()
    {
        CheckNotClosed();
        try
        {
            return _bridge!.JournalLatestSequence();
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public JournalRecord? JournalRead(long sequence)
    {
        CheckNotClosed();
        try
        {
            var result = _bridge!.JournalRead(sequence);
            return result is null ? null : new JournalRecord(result.Sequence, result.Data);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public IReadOnlyList<JournalRecord> JournalQuery(long startSequence, long endSequence)
    {
        CheckNotClosed();
        try
        {
            var results = _bridge!.JournalQuery(startSequence, endSequence);
            var records = new JournalRecord[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                records[i] = new JournalRecord(results[i].Sequence, results[i].Data);
            }
            return records;
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    public Record ReadJournalSourceRecord(ulong datasetIdentifier, JournalIndexInfo indexInfo)
    {
        CheckNotClosed();
        try
        {
            var nativeInfo = new uniffi.timslite.JournalIndexInfo(
                indexInfo.Timestamp,
                indexInfo.BlockOffset,
                indexInfo.InBlockOffset
            );
            var result = _bridge!.ReadJournalSourceRecord(datasetIdentifier, nativeInfo);
            return new Record(result.Timestamp, result.Data);
        }
        catch (uniffi.timslite.TmslException e)
        {
            throw TmslException.FromUniFFI(e);
        }
    }

    private static DataSetInspectResult ConvertInspectResult(uniffi.timslite.DataSetInspectResult result)
    {
        var info = new DataSetInfo(
            result.Info.Name,
            result.Info.DatasetType,
            result.Info.BaseDir,
            result.Info.Identifier,
            result.Info.DataSegmentSize,
            result.Info.IndexSegmentSize,
            result.Info.InitialDataSegmentSize,
            result.Info.InitialIndexSegmentSize,
            result.Info.CompressType,
            result.Info.CompressLevel,
            result.Info.IndexContinuous,
            result.Info.RetentionWindow,
            result.Info.EnableJournal,
            result.Info.CreateTime
        );
        var state = new DataSetState(
            result.State.LatestWrittenTimestamp,
            result.State.OpenDataSegments,
            result.State.DataSegments,
            result.State.TotalRecordCount,
            result.State.TotalDataSize,
            result.State.TotalUncompressedSize,
            result.State.TotalInvalidRecordCount,
            result.State.MinTimestamp,
            result.State.MaxTimestamp,
            result.State.OpenIndexSegments,
            result.State.IndexSegments,
            result.State.PendingIndexEntries,
            result.State.BaseTimestamp,
            result.State.ReadOnly,
            result.State.HasBlockCache,
            result.State.HasJournal,
            result.State.HasQueue,
            result.State.QueueConsumerGroups
        );
        return new DataSetInspectResult(info, state);
    }

    private void CheckNotClosed()
    {
        if (_disposed || _bridge is null)
        {
            throw new ObjectDisposedException(nameof(Store), "Store is closed");
        }
    }
}
