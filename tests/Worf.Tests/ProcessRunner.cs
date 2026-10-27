using System.ComponentModel;
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

        using Process process = await StartAsync(start, cancellationToken);
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

    /// <summary>
    /// Starts a program. A program a test has just copied can't be run while another test's process, forked in the
    /// meantime, still holds the copy open (ETXTBSY), so that is retried briefly.
    /// </summary>
    private static async Task<Process> StartAsync(ProcessStartInfo start, CancellationToken cancellationToken)
    {
        const int TextFileBusy = 26;
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return Process.Start(start) ?? throw new InvalidOperationException($"could not start {start.FileName}");
            }
            catch (Win32Exception error) when (error.NativeErrorCode == TextFileBusy && !OperatingSystem.IsWindows() && attempt < 50)
            {
                await Task.Delay(100, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Reads a program's output to its end. Windows gives a program's output a synchronous pipe, whose asynchronous
    /// reads each hold a thread-pool worker until the program exits, so there a dedicated thread reads it, as Ankus
    /// does; a busy runner adds workers so slowly that the test host's timers and connections otherwise stall.
    /// </summary>
    /// <param name="reader">The program's standard output or error.</param>
    /// <returns>The output.</returns>
    public static Task<string> ReadToEndAsync(StreamReader reader)
        => OperatingSystem.IsWindows()
            ? Task.Factory.StartNew(reader.ReadToEnd, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)
            : reader.ReadToEndAsync(CancellationToken.None);

    private static Task PumpAsync(StreamReader reader, StringBuilder output)
        => OperatingSystem.IsWindows()
            ? Task.Factory.StartNew(() => Pump(reader, output), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default)
            : PumpAsyncCore(reader, output);

    private static void Pump(StreamReader reader, StringBuilder output)
    {
        char[] buffer = new char[4096];
        int read;
        while ((read = reader.Read(buffer)) > 0)
        {
            lock (output)
            {
                _ = output.Append(buffer, 0, read);
            }
        }
    }

    private static async Task PumpAsyncCore(StreamReader reader, StringBuilder output)
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
