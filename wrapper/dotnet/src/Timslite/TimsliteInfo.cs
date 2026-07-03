namespace Timslite;

public static class TimsliteInfo
{
    public static string Version()
    {
        NativeLibraryLoader.Load();
        return uniffi.timslite.TimsliteMethods.Version();
    }
}
