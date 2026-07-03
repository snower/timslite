using System;
using System.IO;
using System.Text;

namespace Timslite.Tests;

public class PersistenceTests : IDisposable
{
    private readonly string _tempDir;

    public PersistenceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "timslite_dotnet_persist_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    [Fact]
    public void Close_Reopen_DataPersists()
    {
        var data = Encoding.UTF8.GetBytes("persistent");

        using (var store = Store.Open(_tempDir))
        {
            store.CreateDataset("persist", "test");
            using var ds = store.OpenDataset("persist", "test");
            ds.Write(42, data);
            ds.Flush();
        }

        using (var store2 = Store.Open(_tempDir))
        {
            using var ds2 = store2.OpenDataset("persist", "test");
            var record = ds2.Read(42);
            Assert.NotNull(record);
            Assert.Equal(42, record!.Timestamp);
            Assert.Equal(data, record.Data);
        }
    }

    [Fact]
    public void Close_Reopen_QueryPersists()
    {
        using (var store = Store.Open(_tempDir))
        {
            store.CreateDataset("persistq", "test");
            using var ds = store.OpenDataset("persistq", "test");
            for (int i = 1; i <= 3; i++)
            {
                ds.Write(i * 10, Encoding.UTF8.GetBytes($"data_{i}"));
            }
            ds.Flush();
        }

        using (var store2 = Store.Open(_tempDir))
        {
            using var ds2 = store2.OpenDataset("persistq", "test");
            var results = ds2.Query(10, 31);
            Assert.Equal(3, results.Count);
        }
    }
}
