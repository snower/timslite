using System;
using System.IO;

namespace Timslite.Tests;

public class LifecycleTests : IDisposable
{
    private readonly string _tempDir;

    public LifecycleTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"timslite_dotnet_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); }
            catch { }
        }
    }

    [Fact]
    public void Open_Close_StoreIsDisposed()
    {
        using var store = Store.Open(_tempDir);
        Assert.False(store.IsClosed);
        store.Dispose();
        Assert.True(store.IsClosed);
    }

    [Fact]
    public void Close_IsIdempotent()
    {
        var store = Store.Open(_tempDir);
        store.Dispose();
        store.Dispose();
        Assert.True(store.IsClosed);
    }

    [Fact]
    public void Methods_ThrowAfterClose()
    {
        var store = Store.Open(_tempDir);
        store.Dispose();

        Assert.Throws<ObjectDisposedException>(() => store.IsReadOnly());
        Assert.Throws<ObjectDisposedException>(() => store.GetDatasetNames());
        Assert.Throws<ObjectDisposedException>(() => store.CreateDataset("a", "b"));
        Assert.Throws<ObjectDisposedException>(() => store.OpenDataset("a", "b"));
        Assert.Throws<ObjectDisposedException>(() => store.DropDataset("a", "b"));
        Assert.Throws<ObjectDisposedException>(() => store.InspectDataset("a", "b"));
        Assert.Throws<ObjectDisposedException>(() => store.TickBackgroundTasks());
        Assert.Throws<ObjectDisposedException>(() => store.NextBackgroundDelayMs());
    }

    [Fact]
    public void Create_Open_Drop_Recreate()
    {
        using var store = Store.Open(_tempDir);

        store.CreateDataset("metrics", "cpu");
        using (var ds = store.OpenDataset("metrics", "cpu"))
        {
            Assert.False(ds.IsClosed);
            Assert.True(ds.Identifier > 0);
        }

        store.DropDataset("metrics", "cpu");

        store.CreateDataset("metrics", "cpu");
        using (var ds = store.OpenDataset("metrics", "cpu"))
        {
            Assert.False(ds.IsClosed);
        }
    }

    [Fact]
    public void OpenDatasetByIdentifier()
    {
        using var store = Store.Open(_tempDir);

        store.CreateDataset("test", "type1");
        var inspectResult = store.InspectDataset("test", "type1");
        var identifier = inspectResult.Info.Identifier;

        using var ds = store.OpenDatasetByIdentifier(identifier);
        Assert.False(ds.IsClosed);
        Assert.Equal(identifier, ds.Identifier);
    }

    [Fact]
    public void GetDatasetNames_Empty()
    {
        using var store = Store.Open(_tempDir);
        var names = store.GetDatasetNames();
        Assert.Empty(names);
    }

    [Fact]
    public void GetDatasetNames_AfterCreate()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("alpha", "t1");
        store.CreateDataset("beta", "t2");

        var names = store.GetDatasetNames();
        Array.Sort(names);
        Assert.Equal(new[] { "alpha", "beta" }, names);
    }

    [Fact]
    public void GetDatasetTypes()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("metrics", "cpu");
        store.CreateDataset("metrics", "mem");

        var types = store.GetDatasetTypes("metrics");
        Array.Sort(types);
        Assert.Equal(new[] { "cpu", "mem" }, types);
    }

    [Fact]
    public void InspectDataset_ReturnsInfoAndState()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("inspect_test", "type_a");

        var result = store.InspectDataset("inspect_test", "type_a");
        Assert.NotNull(result);
        Assert.NotNull(result.Info);
        Assert.NotNull(result.State);

        Assert.Equal("inspect_test", result.Info.Name);
        Assert.Equal("type_a", result.Info.DatasetType);
        Assert.True(result.Info.Identifier > 0);
        Assert.False(string.IsNullOrEmpty(result.Info.BaseDir));
    }

    [Fact]
    public void Dataset_Dispose_ClosesHandle()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("ds_test", "t");

        var ds = store.OpenDataset("ds_test", "t");
        Assert.False(ds.IsClosed);
        ds.Dispose();
        Assert.True(ds.IsClosed);
    }

    [Fact]
    public void Dataset_Identifier_ThrowsAfterClose()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("ds_id_test", "t");

        var ds = store.OpenDataset("ds_id_test", "t");
        ds.Dispose();

        Assert.Throws<ObjectDisposedException>(() => ds.Identifier);
    }

    [Fact]
    public void UsingVar_Example()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("example", "demo");
        using (var ds = store.OpenDataset("example", "demo"))
        {
            Assert.False(ds.IsClosed);
        }
    }
}
