namespace Timslite;

public sealed record JournalIndexInfo(
    long Timestamp,
    ulong BlockOffset,
    ushort InBlockOffset
);
