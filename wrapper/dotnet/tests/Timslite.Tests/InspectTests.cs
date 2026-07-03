using System;
using System.IO;
using System.Text;
using Timslite.Errors;

namespace Timslite.Tests;

public class InspectTests : IDisposable
{
    private readonly string _tempDir;
    private readonly Store _store;

    public InspectTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"timslite_dotnet_inspect_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
        _store = Store.Open(_tempDir);
    }

    public void Dispose()
    {
        _store.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    [Fact]
    public void Inspect_EmptyDataset_ReturnsValidInfo()
    {
        _store.CreateDataset("inspect", "empty");
        var result = _store.InspectDataset("inspect", "empty");

        Assert.Equal("inspect", result.Info.Name);
        Assert.Equal("empty", result.Info.DatasetType);
        Assert.True(result.Info.Identifier > 0);
        Assert.True(result.Info.DataSegmentSize > 0);
        Assert.True(result.Info.IndexSegmentSize > 0);
        Assert.True(result.Info.CreateTime > 0);
    }

    [Fact]
    public void Inspect_EmptyDataset_StateReflectsNoData()
    {
        _store.CreateDataset("inspect", "nodata");
        var result = _store.InspectDataset("inspect", "nodata");

        Assert.Null(result.State.LatestWrittenTimestamp);
        Assert.Equal(0UL, result.State.TotalRecordCount);
        Assert.Equal(0UL, result.State.TotalDataSize);
        Assert.Null(result.State.MinTimestamp);
        Assert.Null(result.State.MaxTimestamp);
    }

    [Fact]
    public void Inspect_AfterWrite_StateReflectsData()
    {
        _store.CreateDataset("inspect", "withdata");
        using var ds = _store.OpenDataset("inspect", "withdata");
        ds.Write(1000, Encoding.UTF8.GetBytes("hello"));
        ds.Write(2000, Encoding.UTF8.GetBytes("world"));
        ds.Flush();

        var result = _store.InspectDataset("inspect", "withdata");

        Assert.Equal(2000L, result.State.LatestWrittenTimestamp);
        Assert.Equal(2UL, result.State.TotalRecordCount);
        Assert.True(result.State.TotalDataSize > 0);
        Assert.Equal(1000L, result.State.MinTimestamp);
        Assert.Equal(2000L, result.State.MaxTimestamp);
    }

    [Fact]
    public void Inspect_GetDatasetNames_ReturnsCreatedDatasets()
    {
        _store.CreateDataset("alpha", "type1");
        _store.CreateDataset("beta", "type2");
        _store.CreateDataset("alpha", "type3");

        var names = _store.GetDatasetNames();
        Assert.Contains("alpha", names);
        Assert.Contains("beta", names);
    }

    [Fact]
    public void Inspect_GetDatasetTypes_ReturnsTypesForName()
    {
        _store.CreateDataset("multi", "typeA");
        _store.CreateDataset("multi", "typeB");

        var types = _store.GetDatasetTypes("multi");
        Assert.Contains("typeA", types);
        Assert.Contains("typeB", types);
    }

    [Fact]
    public void Inspect_NonExistent_ThrowsNotFound()
    {
        Assert.Throws<TmslException>(() => _store.InspectDataset("nonexist", "nonexist"));
    }
}
