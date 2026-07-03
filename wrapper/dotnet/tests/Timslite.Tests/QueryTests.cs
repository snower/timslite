using System;
using System.IO;
using System.Text;

namespace Timslite.Tests;

public class QueryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly Store _store;
    private readonly Dataset _dataset;

    public QueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "timslite_dotnet_query_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _store = Store.Open(_tempDir);
        _store.CreateDataset("test", "query");
        _dataset = _store.OpenDataset("test", "query");
    }

    public void Dispose()
    {
        _dataset.Dispose();
        _store.Dispose();
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    private void WriteSampleData()
    {
        for (int i = 1; i <= 5; i++)
        {
            _dataset.Write(i * 100, Encoding.UTF8.GetBytes($"record_{i}"));
        }
    }

    [Fact]
    public void Query_ReturnsMatchingRecords()
    {
        WriteSampleData();

        var results = _dataset.Query(100, 301);
        Assert.Equal(3, results.Count);
        Assert.Equal(100, results[0].Timestamp);
        Assert.Equal(200, results[1].Timestamp);
        Assert.Equal(300, results[2].Timestamp);
    }

    [Fact]
    public void Query_EmptyRange_ReturnsEmpty()
    {
        WriteSampleData();

        var results = _dataset.Query(9000, 9999);
        Assert.Empty(results);
    }

    [Fact]
    public void Query_Iterator_PartialConsumption()
    {
        WriteSampleData();

        using var iter = _dataset.QueryIter(100, 501);
        Assert.True(iter.MoveNext());
        Assert.Equal(100, iter.Current.Timestamp);

        Assert.True(iter.MoveNext());
        Assert.Equal(200, iter.Current.Timestamp);

        iter.Dispose();
    }

    [Fact]
    public void Query_Iterator_CollectAll()
    {
        WriteSampleData();

        using var iter = _dataset.QueryIter(100, 501);
        var all = iter.CollectAll();
        Assert.Equal(5, all.Count);
    }

    [Fact]
    public void Query_Iterator_CollectTake()
    {
        WriteSampleData();

        using var iter = _dataset.QueryIter(100, 501);
        var taken = iter.CollectTake(3);
        Assert.Equal(3, taken.Count);
        Assert.Equal(100, taken[0].Timestamp);
        Assert.Equal(300, taken[2].Timestamp);
    }

    [Fact]
    public void Query_Iterator_Reverse()
    {
        WriteSampleData();

        using var iter = _dataset.QueryIter(100, 501);
        iter.Reverse();
        Assert.True(iter.MoveNext());
        Assert.Equal(500, iter.Current.Timestamp);
    }

    [Fact]
    public void Query_Iterator_Skip()
    {
        WriteSampleData();

        using var iter = _dataset.QueryIter(100, 501);
        iter.Skip(2);
        Assert.True(iter.MoveNext());
        Assert.Equal(300, iter.Current.Timestamp);
    }

    [Fact]
    public void QueryExist_ReturnsBitmap()
    {
        WriteSampleData();

        var bitmap = _dataset.QueryExist(100, 301);
        Assert.NotEmpty(bitmap);
    }

    [Fact]
    public void QueryLength_ReturnsLengths()
    {
        WriteSampleData();

        var lengths = _dataset.QueryLength(100, 301);
        Assert.Equal(3, lengths.Count);
        foreach (var entry in lengths)
        {
            Assert.True(entry.Length > 0);
        }
    }

    [Fact]
    public void Query_Iterator_Foreach()
    {
        WriteSampleData();

        int count = 0;
        using (var iter = _dataset.QueryIter(100, 501))
        {
            foreach (var record in iter)
            {
                count++;
            }
        }
        Assert.Equal(5, count);
    }
}
