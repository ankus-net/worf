using System.Diagnostics;
using System.Text;

namespace Worf.Tests;

/// <summary>
/// Runs programs and collects their output.
/// </summary>
internal static class ProcessRunner
{
    /// <summary>
    /// Runs a program and waits for it to exit.
    /// </summary>
    /// <remarks>
    /// A server the program starts, as <c>pg_ctl start</c> does, can inherit its output and keep it open, so the
    /// output is awaited only briefly once the program exits. Disposing the process leaves the streams open, so the
    /// pumps go on draining whatever the server writes and it never blocks on a full pipe.
    /// </remarks>
    /// <param name="fileName">The program.</param>
    /// <param name="arguments">The arguments.</param>
    /// <param name="cancellationToken">Cancels waiting.</param>
    /// <param name="workingDirectory">The working directory, or null for the current one.</param>
    /// <param name="environment">Environment variables to set, or null.</param>
    /// <returns>The exit code, standard output and standard error.</returns>
    public static async Task<(int ExitCode, string Stdout, string Stderr)> RunAsync(string fileName, IReadOnlyList<string> arguments,
        CancellationToken cancellationToken, string? workingDirectory = null, IReadOnlyDictionary<string, string>? environment = null)
    {
        ProcessStartInfo start = new(fileName)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        if (workingDirectory is not null)
        {
            start.WorkingDirectory = workingDirectory;
        }

        foreach (string argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        foreach ((string name, string value) in environment ?? new Dictionary<string, string>())
        {
            start.Environment[name] = value;
        }

        using Process process = Process.Start(start) ?? throw new InvalidOperationException($"could not start {fileName}");
        StringBuilder stdout = new();
        StringBuilder stderr = new();
        Task pumps = Task.WhenAll(PumpAsync(process.StandardOutput, stdout), PumpAsync(process.StandardError, stderr));
        await process.WaitForExitAsync(cancellationToken);
        _ = await Task.WhenAny(pumps, Task.Delay(TimeSpan.FromSeconds(2), cancellationToken));
        lock (stdout)
        {
            lock (stderr)
            {
                return (process.ExitCode, stdout.ToString(), stderr.ToString());
            }
        }
    }

    private static async Task PumpAsync(StreamReader reader, StringBuilder output)
    {
        char[] buffer = new char[4096];
        int read;
        while ((read = await reader.ReadAsync(buffer)) > 0)
        {
            lock (output)
            {
                _ = output.Append(buffer, 0, read);
            }
        }
    }
}
