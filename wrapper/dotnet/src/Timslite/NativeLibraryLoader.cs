using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Timslite;

internal static class NativeLibraryLoader
{
    private static bool _loaded;

    public static void Load()
    {
        if (_loaded) return;

        var customPath = Environment.GetEnvironmentVariable("TIMSLITE_NATIVE_LIBRARY_PATH");
        if (!string.IsNullOrEmpty(customPath))
        {
            if (!File.Exists(customPath))
            {
                throw new InvalidOperationException(
                    $"TIMSLITE_NATIVE_LIBRARY_PATH points to a missing file: {customPath}");
            }

            NativeLibrary.Load(customPath);
            _loaded = true;
            return;
        }

        var rid = GetCurrentRid();
        var libName = GetNativeLibraryName(rid);
        var basePath = GetAssemblyDirectory();

        // Search assembly dir and two levels up to cover NuGet restore output layout.
        var searchRoots = new[]
        {
            basePath,
            Path.Combine(basePath, ".."),
            Path.Combine(basePath, "..", ".."),
        };

        foreach (var root in searchRoots)
        {
            var candidate = Path.GetFullPath(Path.Combine(root, "runtimes", rid, "native", libName));
            if (File.Exists(candidate))
            {
                NativeLibrary.Load(candidate);
                _loaded = true;
                return;
            }
        }

        try
        {
            NativeLibrary.Load(libName);
            _loaded = true;
            return;
        }
        catch (DllNotFoundException)
        {
        }

        throw new DllNotFoundException(
            $"Failed to load native library '{libName}' for RID '{rid}'. " +
            $"Searched: {string.Join(", ", Array.ConvertAll(searchRoots, r => Path.GetFullPath(Path.Combine(r, "runtimes", rid, "native"))))}. " +
            $"Set TIMSLITE_NATIVE_LIBRARY_PATH to override the native library path.");
    }

    internal static string GetCurrentRid()
    {
        var os = GetOsId();
        var arch = GetArchId();
        if (os == "linux" && IsMuslRuntime(RuntimeInformation.RuntimeIdentifier))
            return $"linux-musl-{arch}";

        return $"{os}-{arch}";
    }

    internal static bool IsMuslRuntime(string? runtimeIdentifier)
    {
        return !string.IsNullOrEmpty(runtimeIdentifier) &&
               runtimeIdentifier.StartsWith("linux-musl-", StringComparison.OrdinalIgnoreCase);
    }

    internal static string GetNativeLibraryName(string rid)
    {
        if (rid.StartsWith("win-"))
            return "timslite_dotnet.dll";
        if (rid.StartsWith("osx-"))
            return "libtimslite_dotnet.dylib";
        if (rid.StartsWith("linux-"))
            return "libtimslite_dotnet.so";

        throw new PlatformNotSupportedException(
            $"Unsupported RID '{rid}'. Packaged RIDs: win-x64, win-arm64, linux-x64, linux-arm64, " +
            $"linux-musl-x64, linux-musl-arm64, osx-arm64.");
    }

    private static string GetOsId()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return "win";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            return "linux";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            return "osx";

        throw new PlatformNotSupportedException(
            $"Unsupported OS platform. Supported: Windows, Linux, macOS.");
    }

    private static string GetArchId()
    {
        return RuntimeInformation.OSArchitecture switch
        {
            Architecture.X64 => "x64",
            Architecture.Arm64 => "arm64",
            _ => throw new PlatformNotSupportedException(
                $"Unsupported CPU architecture '{RuntimeInformation.OSArchitecture}'. Supported: X64, Arm64.")
        };
    }

    private static string GetAssemblyDirectory()
    {
        var location = Assembly.GetExecutingAssembly().Location;
        return string.IsNullOrEmpty(location)
            ? AppContext.BaseDirectory
            : Path.GetDirectoryName(location) ?? AppContext.BaseDirectory;
    }
}
