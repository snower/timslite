using System;
using System.IO;
using System.Text;

namespace Timslite.Tests;

public class DatasetIoTests : IDisposable
{
    private readonly string _tempDir;
    private readonly Store _store;
    private readonly Dataset _dataset;

    public DatasetIoTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "timslite_dotnet_io_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _store = Store.Open(_tempDir);
        _store.CreateDataset("test", "io");
        _dataset = _store.OpenDataset("test", "io");
    }

    public void Dispose()
    {
        _dataset.Dispose();
        _store.Dispose();
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    [Fact]
    public void Write_And_Read()
    {
        var data = Encoding.UTF8.GetBytes("hello");
        _dataset.Write(1000, data);

        var record = _dataset.Read(1000);
        Assert.NotNull(record);
        Assert.Equal(1000, record!.Timestamp);
        Assert.Equal(data, record.Data);
    }

    [Fact]
    public void Read_NonExistent_ReturnsNull()
    {
        var record = _dataset.Read(9999);
        Assert.Null(record);
    }

    [Fact]
    public void WriteNow()
    {
        var data = Encoding.UTF8.GetBytes("now");
        _dataset.WriteNow(data);

        var latest = _dataset.ReadLatest();
        Assert.NotNull(latest);
        Assert.Equal(data, latest!.Data);
    }

    [Fact]
    public void Append_CreatesNewRecord()
    {
        var data1 = Encoding.UTF8.GetBytes("first");
        _dataset.Append(2000, data1);

        var record = _dataset.Read(2000);
        Assert.NotNull(record);
        Assert.Equal(data1, record!.Data);
    }

    [Fact]
    public void AppendNow()
    {
        var data = Encoding.UTF8.GetBytes("append_now");
        _dataset.AppendNow(data);

        var latest = _dataset.ReadLatest();
        Assert.NotNull(latest);
        Assert.Equal(data, latest!.Data);
    }

    [Fact]
    public void Delete_ThenRead_ReturnsNull()
    {
        _dataset.Write(3000, new byte[] { 1, 2, 3 });
        Assert.NotNull(_dataset.Read(3000));

        _dataset.Delete(3000);
        Assert.Null(_dataset.Read(3000));
    }

    [Fact]
    public void ReadLatest_ReturnsMaxTimestamp()
    {
        _dataset.Write(100, new byte[] { 1 });
        _dataset.Write(200, new byte[] { 2 });
        _dataset.Write(300, new byte[] { 3 });

        var latest = _dataset.ReadLatest();
        Assert.NotNull(latest);
        Assert.Equal(300, latest!.Timestamp);
    }

    [Fact]
    public void Flush_DoesNotThrow()
    {
        _dataset.Write(5000, new byte[] { 1 });
        _dataset.Flush();
    }

    [Fact]
    public void ReadExist_ReturnsTrueForWritten()
    {
        _dataset.Write(6000, new byte[] { 1 });
        Assert.True(_dataset.ReadExist(6000));
    }

    [Fact]
    public void ReadExist_ReturnsFalseForMissing()
    {
        Assert.False(_dataset.ReadExist(7000));
    }

    [Fact]
    public void ReadLength_ReturnsLength()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        _dataset.Write(8000, data);

        var length = _dataset.ReadLength(8000);
        Assert.NotNull(length);
        Assert.Equal((uint)5, length!.Value);
    }

    [Fact]
    public void ReadLength_ReturnsNullForMissing()
    {
        var length = _dataset.ReadLength(9000);
        Assert.Null(length);
    }
}
