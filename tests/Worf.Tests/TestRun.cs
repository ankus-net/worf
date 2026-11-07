namespace Worf.Tests;

/// <summary>
/// Cleans up what the test run's tests shared, once they've all finished.
/// </summary>
[TestClass]
public static class TestRun
{
    /// <summary>
    /// Deletes the publication of worf and the staged installation that the test run's nodes shared.
    /// </summary>
    /// <returns>The work.</returns>
    [AssemblyCleanup]
    public static Task CleanupAsync() => TestNode.DisposeSharedAsync();
}
