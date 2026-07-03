namespace Timslite.Tests;

public class SmokeTests
{
    [Fact]
    public void Version_ReturnsNonEmpty()
    {
        var version = TimsliteInfo.Version();
        Assert.False(string.IsNullOrEmpty(version));
    }
}
