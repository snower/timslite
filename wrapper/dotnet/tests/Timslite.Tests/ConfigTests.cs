using System;
using System.IO;

namespace Timslite.Tests;

public class ConfigTests : IDisposable
{
    private readonly string _tempDir;

    public ConfigTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"timslite_dotnet_config_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    [Fact]
    public void StoreConfig_Defaults_OpenSucceeds()
    {
        using var store = Store.Open(_tempDir, new StoreConfig());
        Assert.False(store.IsClosed);
    }

    [Fact]
    public void StoreConfig_CustomValues_OpenSucceeds()
    {
        var config = new StoreConfig
        {
            FlushIntervalSeconds = 60,
            IdleTimeoutSeconds = 30,
            DataSegmentSize = 8 * 1024 * 1024,
            IndexSegmentSize = 4 * 1024 * 1024,
            CompressLevel = 6,
            CacheMaxMemory = 16 * 1024 * 1024,
            CacheIdleTimeoutSeconds = 120,
            RetentionCheckHour = 0,
            EnableBackgroundThread = false,
            EnableJournal = true,
            ReadOnly = false,
        };
        using var store = Store.Open(_tempDir, config);
        Assert.False(store.IsClosed);
    }

    [Fact]
    public void StoreConfig_ReadOnly_CanOpen()
    {
        using (var store = Store.Open(_tempDir))
        {
            store.CreateDataset("ro", "test");
            using var ds = store.OpenDataset("ro", "test");
            ds.Write(100, new byte[] { 1, 2, 3 });
            ds.Flush();
        }

        var roConfig = new StoreConfig { ReadOnly = true };
        using var roStore = Store.Open(_tempDir, roConfig);
        Assert.True(roStore.IsReadOnly());
    }

    [Fact]
    public void DatasetConfig_Custom_CreateSucceeds()
    {
        var dsConfig = new DatasetConfig
        {
            DataSegmentSize = 2 * 1024 * 1024,
            IndexSegmentSize = 1024 * 1024,
            CompressLevel = 3,
            RetentionWindow = 0,
            EnableJournal = false,
        };
        var options = new CreateDatasetOptions { Config = dsConfig };

        using var store = Store.Open(_tempDir);
        store.CreateDataset("custom", "ds", options);
        using var ds = store.OpenDataset("custom", "ds");
        Assert.False(ds.IsClosed);
    }

    [Fact]
    public void CreateDatasetOptions_DefaultConfig_CreateSucceeds()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("defcfg", "test", new CreateDatasetOptions());
        using var ds = store.OpenDataset("defcfg", "test");
        Assert.False(ds.IsClosed);
    }

    [Fact]
    public void CreateDatasetOptions_NullConfig_CreateSucceeds()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("nullcfg", "test", new CreateDatasetOptions { Config = null });
        using var ds = store.OpenDataset("nullcfg", "test");
        Assert.False(ds.IsClosed);
    }
}
