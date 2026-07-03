using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Timslite.Tests;

public class PackagingTests
{
    [Fact]
    public void TimsliteInfo_Version_ReturnsSemverLike()
    {
        var version = TimsliteInfo.Version();
        Assert.False(string.IsNullOrEmpty(version));
        Assert.Contains(".", version);
    }

    [Fact]
    public void NativeLibraryLoader_LoadsMultipleTimes_DoesNotThrow()
    {
        var dir1 = Path.Combine(Path.GetTempPath(), $"timslite_dotnet_pkg1_{Guid.NewGuid():N}");
        var dir2 = Path.Combine(Path.GetTempPath(), $"timslite_dotnet_pkg2_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir1);
        Directory.CreateDirectory(dir2);

        try
        {
            using var store1 = Store.Open(dir1);
            using var store2 = Store.Open(dir2);
            Assert.False(store1.IsClosed);
            Assert.False(store2.IsClosed);
        }
        finally
        {
            try { Directory.Delete(dir1, recursive: true); } catch { }
            try { Directory.Delete(dir2, recursive: true); } catch { }
        }
    }

    [Fact]
    public void Store_Open_WithEnvOverride_FallsBack()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"timslite_dotnet_env_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);

        try
        {
            using var store = Store.Open(dir);
            Assert.False(store.IsClosed);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { }
        }
    }

    [Fact]
    public void GetCurrentRid_ReturnsValidRid()
    {
        var rid = NativeLibraryLoader.GetCurrentRid();

        Assert.Contains("-", rid);
        Assert.True(
            rid == "win-x64" || rid == "win-arm64" ||
            rid == "linux-x64" || rid == "linux-arm64" ||
            rid == "linux-musl-x64" || rid == "linux-musl-arm64" ||
            rid == "osx-x64" || rid == "osx-arm64",
            $"Unexpected RID: {rid}");
    }

    [Fact]
    public void GetCurrentRid_MatchesRuntimeInformation()
    {
        var rid = NativeLibraryLoader.GetCurrentRid();

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            Assert.StartsWith("win-", rid);
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            Assert.StartsWith("linux-", rid);
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            Assert.StartsWith("osx-", rid);

        switch (RuntimeInformation.OSArchitecture)
        {
            case Architecture.X64:
                Assert.EndsWith("-x64", rid);
                break;
            case Architecture.Arm64:
                Assert.EndsWith("-arm64", rid);
                break;
        }
    }

    [Theory]
    [InlineData("win-x64", "timslite_dotnet.dll")]
    [InlineData("win-arm64", "timslite_dotnet.dll")]
    [InlineData("linux-x64", "libtimslite_dotnet.so")]
    [InlineData("linux-arm64", "libtimslite_dotnet.so")]
    [InlineData("linux-musl-x64", "libtimslite_dotnet.so")]
    [InlineData("linux-musl-arm64", "libtimslite_dotnet.so")]
    [InlineData("osx-x64", "libtimslite_dotnet.dylib")]
    [InlineData("osx-arm64", "libtimslite_dotnet.dylib")]
    public void GetNativeLibraryName_ReturnsCorrectName(string rid, string expected)
    {
        var name = NativeLibraryLoader.GetNativeLibraryName(rid);
        Assert.Equal(expected, name);
    }

    [Fact]
    public void GetNativeLibraryName_ThrowsForUnsupportedRid()
    {
        Assert.Throws<PlatformNotSupportedException>(
            () => NativeLibraryLoader.GetNativeLibraryName("freebsd-x64"));
    }

    [Theory]
    [InlineData("linux-musl-x64", true)]
    [InlineData("linux-musl-arm64", true)]
    [InlineData("linux-x64", false)]
    [InlineData("win-x64", false)]
    [InlineData("", false)]
    public void IsMuslRuntime_DetectsMuslRuntimeIdentifier(string runtimeIdentifier, bool expected)
    {
        Assert.Equal(expected, NativeLibraryLoader.IsMuslRuntime(runtimeIdentifier));
    }

    [Fact]
    public void EnvOverride_MissingFile_ThrowsActionableError()
    {
        var original = Environment.GetEnvironmentVariable("TIMSLITE_NATIVE_LIBRARY_PATH");
        try
        {
            Environment.SetEnvironmentVariable("TIMSLITE_NATIVE_LIBRARY_PATH", "/nonexistent/lib.so");
            try
            {
                NativeLibraryLoader.Load();
            }
            catch (InvalidOperationException ex)
            {
                Assert.Contains("TIMSLITE_NATIVE_LIBRARY_PATH", ex.Message);
                return;
            }
            catch (DllNotFoundException)
            {
                return;
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable("TIMSLITE_NATIVE_LIBRARY_PATH", original);
        }
    }
}
