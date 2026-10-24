using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using Ankus.PgConfig;

namespace Worf.Tests;

/// <summary>
/// Publishes <c>worf_create_subscriber</c> and runs it from a PostgreSQL installation's programs, as an installed copy
/// would.
/// </summary>
internal static class CreateSubscriberCommand
{
    private static readonly Lazy<Task<string>> s_published = new(PublishAsync);

    /// <summary>
    /// Gets the command's file name.
    /// </summary>
    public static string FileName => OperatingSystem.IsWindows() ? "worf_create_subscriber.exe" : "worf_create_subscriber";

    /// <summary>
    /// Makes a directory where the command finds the installation's <c>pg_ctl</c> and <c>pg_basebackup</c> beside it,
    /// as it does when installed in the installation's bin directory.
    /// </summary>
    /// <param name="installation">The installation.</param>
    /// <param name="directory">The directory to fill.</param>
    /// <returns>The command's path in the directory.</returns>
    public static async Task<string> InstallAsync(PostgresInstallation installation, string directory)
    {
        string published = await s_published.Value;
        Directory.CreateDirectory(directory);
        string command = Path.Join(directory, FileName);
        File.Copy(published, command, overwrite: true);

        if (OperatingSystem.IsWindows())
        {
            // A Windows program finds its libraries in its own directory, and PostgreSQL finds lib and share beside
            // it, so the installation's programs are copied and its lib and share joined beside them.
            foreach (string file in Directory.GetFiles(installation.BinDirectory))
            {
                File.Copy(file, Path.Join(directory, Path.GetFileName(file)), overwrite: true);
            }

            string installationRoot = Path.GetDirectoryName(installation.BinDirectory.TrimEnd(Path.DirectorySeparatorChar))!;
            string root = Path.GetDirectoryName(directory.TrimEnd(Path.DirectorySeparatorChar))!;
            foreach (string name in (string[])["lib", "share"])
            {
                await CreateJunctionAsync(Path.Join(root, name), Path.Join(installationRoot, name));
            }
        }
        else
        {
            // Unix programs resolve their links to find the rest of the installation.
            foreach (string program in (string[])["pg_ctl", "pg_basebackup"])
            {
                File.CreateSymbolicLink(Path.Join(directory, program), Path.Join(installation.BinDirectory, program));
            }
        }

        return command;
    }

    /// <summary>
    /// Runs the command and waits for it.
    /// </summary>
    /// <param name="command">The command's path.</param>
    /// <param name="arguments">The arguments.</param>
    /// <param name="workingDirectory">The working directory, where the command writes its server log.</param>
    /// <param name="environment">Environment variables to set.</param>
    /// <param name="cancellationToken">Cancels waiting.</param>
    /// <returns>The exit code, standard output and standard error.</returns>
    public static Task<(int ExitCode, string Stdout, string Stderr)> RunAsync(string command, IReadOnlyList<string> arguments,
        string workingDirectory, IReadOnlyDictionary<string, string>? environment, CancellationToken cancellationToken)
        => ProcessRunner.RunAsync(command, arguments, cancellationToken, workingDirectory, environment);

    /// <summary>
    /// Finds a TCP port nothing listens on.
    /// </summary>
    /// <returns>The port.</returns>
    public static int FreePort()
    {
        using TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static async Task CreateJunctionAsync(string link, string target)
    {
        (int exitCode, string stdout, string stderr) = await ProcessRunner.RunAsync("cmd.exe", ["/c", "mklink", "/J", link, target],
            CancellationToken.None);
        if (exitCode != 0)
        {
            throw new InvalidOperationException($"mklink /J {link} {target} failed: {stdout}{stderr}");
        }
    }

    private static async Task<string> PublishAsync()
    {
        string output = Path.Join(RepositoryPaths.Root, "artifacts", "worf-create-subscriber", RuntimeInformation.RuntimeIdentifier);
        (int exitCode, string stdout, string stderr) = await ProcessRunner.RunAsync("dotnet",
        [
            "publish", Path.Join(RepositoryPaths.Root, "src", "Worf.CreateSubscriber", "Worf.CreateSubscriber.csproj"),
            "-c", "Release", "-r", RuntimeInformation.RuntimeIdentifier, "-o", output,
        ], CancellationToken.None);
        if (exitCode != 0)
        {
            throw new InvalidOperationException($"publishing worf_create_subscriber failed: {stdout}{stderr}");
        }

        return Path.Join(output, FileName);
    }
}
