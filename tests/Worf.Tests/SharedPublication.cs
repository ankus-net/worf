using Ankus.PgConfig;
using Ankus.Testing;
using Npgsql;

namespace Worf.Tests;

/// <summary>
/// The publication of worf that a test run's nodes share, with the installation they run from, the settings that load
/// worf from them, and where the nodes' server logs go. Before PostgreSQL 18 the installation is a staged copy of the
/// one the run selects with worf's files in it, and from 18 the selected one, with extension_control_path naming worf's
/// control files.
/// </summary>
internal sealed class SharedPublication : IAsyncDisposable
{
    // What owns the publication's files: the harness's fixture, or a staged installation and the directory it's in.
    private readonly IAsyncDisposable? _owner;
    private readonly IDisposable? _directory;

    private SharedPublication(PostgresInstallation installation, IReadOnlyList<string> settings, string directory, string logDirectory,
        IAsyncDisposable? owner, IDisposable? root)
    {
        Installation = installation;
        Settings = settings;
        Directory = directory;
        LogDirectory = logDirectory;
        _owner = owner;
        _directory = root;
    }

    /// <summary>
    /// Gets the installation the nodes run from.
    /// </summary>
    public PostgresInstallation Installation { get; }

    /// <summary>
    /// Gets the settings that load worf from the publication.
    /// </summary>
    public IReadOnlyList<string> Settings { get; }

    /// <summary>
    /// Gets the publication's directory, with worf's library and the manifest of its control and SQL files.
    /// </summary>
    public string Directory { get; }

    /// <summary>
    /// Gets the directory the nodes' server logs go to, which outlasts the run.
    /// </summary>
    public string LogDirectory { get; }

    /// <summary>
    /// Shares the publication in the directory <c>WORF_TEST_PUBLICATION</c> names, absolute or from the repository's
    /// root, as CI's build of worf for the run's PostgreSQL leaves it, or otherwise one the harness publishes.
    /// </summary>
    /// <param name="cancellationToken">Cancels publishing and staging.</param>
    /// <returns>The shared publication.</returns>
    public static Task<SharedPublication> StartAsync(CancellationToken cancellationToken)
        => Environment.GetEnvironmentVariable("WORF_TEST_PUBLICATION") is { Length: > 0 } directory
            ? FromDirectoryAsync(Path.GetFullPath(directory, RepositoryPaths.Root), cancellationToken)
            : FromHarnessAsync(cancellationToken);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_owner is not null)
        {
            await _owner.DisposeAsync();
        }

        _directory?.Dispose();
    }

    /// <summary>
    /// Shares a publication already made, with no build: its library loads from where it is, and its control and SQL
    /// files go into a staged copy of the installation the run selects before PostgreSQL 18, while from 18
    /// extension_control_path names the publication's own, which it keeps under <c>extension</c> as a share directory
    /// does.
    /// </summary>
    private static async Task<SharedPublication> FromDirectoryAsync(string directory, CancellationToken cancellationToken)
    {
        PostgresInstallation installation = await TestInstallations.CurrentAsync(cancellationToken);
        TemporaryDirectory? root = null;
        PostgresTestInstallation? staged = null;
        try
        {
            List<string> settings = [Setting("shared_preload_libraries", "worf"), Setting("dynamic_library_path", $"{directory}{Path.PathSeparator}$libdir")];
            if (installation.Version.Major < 18)
            {
                root = new($"worf-{Guid.NewGuid():N}"[..13], Environment.GetEnvironmentVariable("ANKUS_TEST_DATA_DIRECTORY"));
                staged = await PostgresTestInstallation.StageAsync(installation, Path.Join(root.FullName, "pg"), cancellationToken);
                staged.InstallExtensionFiles(directory);
                installation = staged.Installation;
            }
            else
            {
                settings.Add(Setting("extension_control_path", $"{directory}{Path.PathSeparator}$system"));
            }

            if (File.ReadAllText(Path.Join(installation.SharedDirectory, "postgresql.conf.sample")).Contains("output_plugin_libraries", StringComparison.Ordinal))
            {
                settings.Add(OutputPlugins);
            }

            return new SharedPublication(installation, settings, directory,
                Path.Join(Path.GetDirectoryName(RepositoryPaths.ExtensionProject), "bin", "ankus-test-logs"), staged, root);
        }
        catch
        {
            if (staged is not null)
            {
                await staged.DisposeAsync();
            }

            root?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Shares the publication of a harness fixture, which builds and publishes worf, and stages the installation before
    /// PostgreSQL 18, and whose own settings load worf from them. Its server stops once they're read, as no test uses
    /// it.
    /// </summary>
    private static async Task<SharedPublication> FromHarnessAsync(CancellationToken cancellationToken)
    {
        PostgresExtensionTest fixture = await PostgresExtensionTest.StartAsync(new PostgresExtensionTestOptions
        {
            ProjectPath = RepositoryPaths.ExtensionProject,
            SharedPreload = true,
        }, cancellationToken);
        try
        {
            List<string> settings = [];
            string directory = "";
            await using (NpgsqlConnection connection = await fixture.Cluster.OpenConnectionAsync(cancellationToken))
            await using (NpgsqlCommand read = new("""
                SELECT name, setting FROM pg_catalog.pg_settings
                WHERE name IN ('shared_preload_libraries', 'dynamic_library_path', 'extension_control_path')
                UNION ALL
                SELECT name, '' FROM pg_catalog.pg_settings WHERE name = 'output_plugin_libraries'
                """, connection))
            await using (NpgsqlDataReader reader = await read.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    string name = reader.GetString(0);
                    if (name == "dynamic_library_path")
                    {
                        directory = reader.GetString(1).Split(Path.PathSeparator)[0];
                    }

                    settings.Add(name == "output_plugin_libraries" ? OutputPlugins : Setting(name, reader.GetString(1)));
                }
            }

            (int exitCode, string stdout, string stderr) = await ProcessRunner.RunAsync(fixture.Cluster.Installation.PgCtlPath,
                ["stop", "-D", fixture.Cluster.DataDirectory, "-m", "fast", "-w"], cancellationToken);
            if (exitCode != 0)
            {
                throw new InvalidOperationException($"pg_ctl stop failed: {stdout}{stderr}");
            }

            return new SharedPublication(fixture.Cluster.Installation, settings, directory, Path.GetDirectoryName(fixture.Cluster.LogFilePath)!,
                fixture, null);
        }
        catch
        {
            await fixture.DisposeAsync();
            throw;
        }
    }

    // worf in output_plugin_libraries, which current PostgreSQL releases require of every output plugin, as a DBA lists it.
    private static string OutputPlugins => Setting("output_plugin_libraries", "pgoutput, test_decoding, worf");

    // A setting's quoted value takes backslashes, as in Windows paths, and quotes doubled.
    private static string Setting(string name, string value)
        => $"{name} = '{value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("'", "''", StringComparison.Ordinal)}'";
}
