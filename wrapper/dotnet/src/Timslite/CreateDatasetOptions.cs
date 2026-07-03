namespace Timslite;

/// <summary>
/// Options for creating a dataset, optionally with custom config.
/// </summary>
public sealed record CreateDatasetOptions
{
    public DatasetConfig? Config { get; init; }

    internal uniffi.timslite.CreateDatasetOptions ToNative()
    {
        return new uniffi.timslite.CreateDatasetOptions(
            Config: Config?.ToNative()
        );
    }
}
