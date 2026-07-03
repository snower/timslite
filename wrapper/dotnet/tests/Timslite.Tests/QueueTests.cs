using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Timslite.Tests;

public class QueueTests : IDisposable
{
    private readonly string _tempDir;
    private readonly Store _store;
    private readonly Dataset _dataset;

    public QueueTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "timslite_dotnet_queue_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _store = Store.Open(_tempDir);
        _store.CreateDataset("test", "queue");
        _dataset = _store.OpenDataset("test", "queue");
    }

    public void Dispose()
    {
        _dataset.Dispose();
        _store.Dispose();
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    [Fact]
    public void OpenConsumer_BeforePush_ThenPoll()
    {
        using var queue = _store.OpenQueue(_dataset);
        using var consumer = queue.OpenConsumer("group1");

        var data = Encoding.UTF8.GetBytes("hello");
        queue.Push(data);

        var record = consumer.Poll(TimeSpan.FromSeconds(5));
        Assert.NotNull(record);
        Assert.Equal(data, record!.Data);
    }

    [Fact]
    public void Ack_PreventsRedelivery()
    {
        using var queue = _store.OpenQueue(_dataset);
        using var consumer = queue.OpenConsumer("group_ack");

        var data = Encoding.UTF8.GetBytes("ack_test");
        queue.Push(data);

        var record = consumer.Poll(TimeSpan.FromSeconds(5));
        Assert.NotNull(record);
        consumer.Ack(record!.Timestamp);

        var record2 = consumer.Poll(TimeSpan.FromMilliseconds(500));
        Assert.Null(record2);
    }

    [Fact]
    public void Poll_Timeout_ReturnsNull()
    {
        using var queue = _store.OpenQueue(_dataset);
        using var consumer = queue.OpenConsumer("group_timeout");

        var record = consumer.Poll(TimeSpan.FromMilliseconds(100));
        Assert.Null(record);
    }

    [Fact]
    public async Task PollAsync_DoesNotBlockCaller()
    {
        using var queue = _store.OpenQueue(_dataset);
        using var consumer = queue.OpenConsumer("group_async");

        var data = Encoding.UTF8.GetBytes("async_test");
        queue.Push(data);

        var record = await consumer.PollAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(record);
        Assert.Equal(data, record!.Data);
    }

    [Fact]
    public async Task PollAsync_Timeout_ReturnsNull()
    {
        using var queue = _store.OpenQueue(_dataset);
        using var consumer = queue.OpenConsumer("group_async_timeout");

        var record = await consumer.PollAsync(TimeSpan.FromMilliseconds(100));
        Assert.Null(record);
    }

    [Fact]
    public void Consumer_Inspect_ExposesInfoAndState()
    {
        using var queue = _store.OpenQueue(_dataset);
        using var consumer = queue.OpenConsumer("group_inspect");

        var result = consumer.Inspect();
        Assert.NotNull(result);
        Assert.Equal("group_inspect", result.Info.GroupName);
    }

    [Fact]
    public void GetConsumerGroupNames()
    {
        using var queue = _store.OpenQueue(_dataset);
        queue.OpenConsumer("group_a").Dispose();
        queue.OpenConsumer("group_b").Dispose();

        var names = queue.GetConsumerGroupNames();
        Assert.Contains("group_a", names);
        Assert.Contains("group_b", names);
    }

    [Fact]
    public void DropConsumer()
    {
        using var queue = _store.OpenQueue(_dataset);
        queue.OpenConsumer("group_drop").Dispose();

        queue.DropConsumer("group_drop");
        var names = queue.GetConsumerGroupNames();
        Assert.DoesNotContain("group_drop", names);
    }

    [Fact]
    public void Push_ReturnsTimestamp()
    {
        using var queue = _store.OpenQueue(_dataset);
        var ts = queue.Push(Encoding.UTF8.GetBytes("ts"));
        Assert.True(ts > 0);
    }

    [Fact]
    public void Consumer_Flush()
    {
        using var queue = _store.OpenQueue(_dataset);
        using var consumer = queue.OpenConsumer("group_flush");

        queue.Push(Encoding.UTF8.GetBytes("flush_data"));
        var record = consumer.Poll(TimeSpan.FromSeconds(5));
        Assert.NotNull(record);
        consumer.Ack(record!.Timestamp);
        consumer.Flush();
    }
}
