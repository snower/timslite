using System;
using System.IO;
using Timslite.Errors;

namespace Timslite.Tests;

public class ErrorTests : IDisposable
{
    private readonly string _tempDir;

    public ErrorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"timslite_dotnet_err_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    [Fact]
    public void AlreadyExists_DuplicateCreate_Throws()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("dup", "type");

        var ex = Assert.Throws<TmslException>(() => store.CreateDataset("dup", "type"));
        Assert.Equal(TmslErrorCode.AlreadyExists, ex.Code);
    }

    [Fact]
    public void NotFound_OpenNonExistent_Throws()
    {
        using var store = Store.Open(_tempDir);

        var ex = Assert.Throws<TmslException>(() => store.OpenDataset("ghost", "type"));
        Assert.Equal(TmslErrorCode.NotFound, ex.Code);
    }

    [Fact]
    public void NotFound_DropNonExistent_Throws()
    {
        using var store = Store.Open(_tempDir);

        var ex = Assert.Throws<TmslException>(() => store.DropDataset("ghost", "type"));
        Assert.Equal(TmslErrorCode.NotFound, ex.Code);
    }

    [Fact]
    public void NotFound_DeleteNonExistent_Throws()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("errdel", "type");
        using var ds = store.OpenDataset("errdel", "type");

        var ex = Assert.Throws<TmslException>(() => ds.Delete(99999));
        Assert.Equal(TmslErrorCode.NotFound, ex.Code);
    }

    [Fact]
    public void ObjectDisposed_Store_ThrowsAfterDispose()
    {
        var store = Store.Open(_tempDir);
        store.Dispose();

        Assert.Throws<ObjectDisposedException>(() => store.GetDatasetNames());
    }

    [Fact]
    public void ObjectDisposed_Dataset_ThrowsAfterDispose()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("disp", "type");
        var ds = store.OpenDataset("disp", "type");
        ds.Dispose();

        Assert.Throws<ObjectDisposedException>(() => ds.Read(100));
    }

    [Fact]
    public void TmslException_HasMessage()
    {
        using var store = Store.Open(_tempDir);
        store.CreateDataset("errmsg", "type");

        var ex = Assert.Throws<TmslException>(() => store.CreateDataset("errmsg", "type"));
        Assert.False(string.IsNullOrEmpty(ex.Message));
    }

    [Fact]
    public void InvalidData_EmptyName_Throws()
    {
        using var store = Store.Open(_tempDir);

        // Empty dataset name should fail validation
        Assert.ThrowsAny<Exception>(() => store.CreateDataset("", "type"));
    }
}
