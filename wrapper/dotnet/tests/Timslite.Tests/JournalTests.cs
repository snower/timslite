using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Timslite.Tests;

public class JournalTests : IDisposable
{
    private readonly string _tempDir;
    private readonly Store _store;
    private static readonly CreateDatasetOptions JournalOptions = new()
    {
        Config = new DatasetConfig { EnableJournal = true }
    };

    public JournalTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "timslite_dotnet_journal_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        var config = new StoreConfig { EnableJournal = true };
        _store = Store.Open(_tempDir, config);
    }

    public void Dispose()
    {
        _store.Dispose();
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    [Fact]
    public void JournalLatestSequence_InitiallyNull()
    {
        var seq = _store.JournalLatestSequence();
        Assert.Null(seq);
    }

    [Fact]
    public void CreateDataset_CreatesJournalSequence()
    {
        _store.CreateDataset("ds1", "type1", JournalOptions);
        _store.TickBackgroundTasks();
        var seq = _store.JournalLatestSequence();
        Assert.NotNull(seq);
        Assert.True(seq!.Value > 0);
    }

    [Fact]
    public void Write_CreatesJournalSequence()
    {
        _store.CreateDataset("ds_write", "type1", JournalOptions);
        var ds = _store.OpenDataset("ds_write", "type1");
        ds.Write(1000, Encoding.UTF8.GetBytes("data"));
        ds.Flush();
        _store.TickBackgroundTasks();
        var seq = _store.JournalLatestSequence();
        Assert.NotNull(seq);
        Assert.True(seq!.Value > 1);
    }

    [Fact]
    public void JournalRead_ReturnsWrittenRecord()
    {
        _store.CreateDataset("ds_read", "type1", JournalOptions);
        _store.TickBackgroundTasks();
        var latest = _store.JournalLatestSequence();
        Assert.NotNull(latest);

        var record = _store.JournalRead(latest!.Value);
        Assert.NotNull(record);
        Assert.Equal(latest.Value, record!.Sequence);
    }

    [Fact]
    public void JournalQuery_ReturnsRecords()
    {
        _store.CreateDataset("ds_query", "type1", JournalOptions);
        var ds = _store.OpenDataset("ds_query", "type1");
        ds.Write(1000, Encoding.UTF8.GetBytes("q1"));
        ds.Write(2000, Encoding.UTF8.GetBytes("q2"));
        ds.Flush();
        _store.TickBackgroundTasks();

        var latest = _store.JournalLatestSequence();
        Assert.NotNull(latest);

        var records = _store.JournalQuery(1, latest!.Value);
        Assert.True(records.Count >= 3);
    }

    [Fact]
    public void JournalRead_NonExistent_ReturnsNull()
    {
        var record = _store.JournalRead(999999);
        Assert.Null(record);
    }

    [Fact]
    public void ReadJournalSourceRecord_Works()
    {
        _store.CreateDataset("ds_source", "type1", JournalOptions);
        var ds = _store.OpenDataset("ds_source", "type1");
        var data = Encoding.UTF8.GetBytes("source_data");
        ds.Write(5000, data);
        ds.Flush();
        _store.TickBackgroundTasks();

        var dsInfo = _store.InspectDataset("ds_source", "type1");
        var identifier = dsInfo.Info.Identifier;

        var latest = _store.JournalLatestSequence();
        Assert.NotNull(latest);

        var sourceRecord = _store.ReadJournalSourceRecord(
            identifier,
            new JournalIndexInfo(5000, 0, 0)
        );
        Assert.NotNull(sourceRecord);
        Assert.Equal(5000, sourceRecord.Timestamp);
        Assert.Equal(data, sourceRecord.Data);
    }

    [Fact]
    public void JournalQueue_ConsumeRecords()
    {
        _store.CreateDataset("ds_jq", "type1", JournalOptions);
        _store.TickBackgroundTasks();

        using var journalQueue = _store.OpenJournalQueue();
        using var consumer = journalQueue.OpenConsumer("jgroup1");

        var ds = _store.OpenDataset("ds_jq", "type1");
        ds.Write(1000, Encoding.UTF8.GetBytes("jq_data"));
        ds.Flush();
        _store.TickBackgroundTasks();

        var record = consumer.Poll(TimeSpan.FromSeconds(5));
        Assert.NotNull(record);
        Assert.True(record!.Sequence > 0);
        consumer.Ack(record.Sequence);
    }

    [Fact]
    public async Task JournalQueueConsumer_PollAsync()
    {
        _store.CreateDataset("ds_jq_async", "type1", JournalOptions);
        _store.TickBackgroundTasks();

        using var journalQueue = _store.OpenJournalQueue();
        using var consumer = journalQueue.OpenConsumer("jgroup_async");

        var ds = _store.OpenDataset("ds_jq_async", "type1");
        ds.Write(1000, Encoding.UTF8.GetBytes("jq_async_data"));
        ds.Flush();
        _store.TickBackgroundTasks();

        var record = await consumer.PollAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(record);
        Assert.True(record!.Sequence > 0);
        consumer.Ack(record.Sequence);
    }

    [Fact]
    public void JournalQueue_Poll_Timeout_ReturnsNull()
    {
        using var journalQueue = _store.OpenJournalQueue();
        using var consumer = journalQueue.OpenConsumer("jgroup_timeout");

        var record = consumer.Poll(TimeSpan.FromMilliseconds(100));
        Assert.Null(record);
    }
}
