using System.Diagnostics;

namespace Worf.Tests;

/// <summary>
/// A tool a test runs and stops itself, which is killed with its children as the test ends, if it's still running, as
/// when a check fails before the test stopped it.
/// </summary>
/// <param name="process">The tool's process.</param>
internal sealed class RunningTool(Process process) : IAsyncDisposable
{
    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (!process.HasExited)
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(CancellationToken.None);
        }
    }
}
