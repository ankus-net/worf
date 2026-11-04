using System.Diagnostics;
using System.Globalization;
using System.Text;
using Ankus.PgConfig;
using Ankus.Testing;
using Npgsql;

namespace Worf.Tests;

/// <summary>
/// A PostgreSQL cluster with worf loaded, as the Perl tests' nodes are.
/// </summary>
internal sealed class TestNode : IAsyncDisposable
{
    /// <summary>
    /// How long a poll waits for its condition, as PostgreSQL's TAP tests do.
    /// </summary>
    internal static TimeSpan PollTimeout { get; } = TimeSpan.FromSeconds(180);

    private readonly PostgresExtensionTest _extension;

    private TestNode(string name, PostgresExtensionTest extension)
    {
        Name = name;
        _extension = extension;
    }

    /// <summary>
    /// Gets the node's name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the node's cluster.
    /// </summary>
    public PostgresTestCluster Cluster => _extension.Cluster;

    /// <summary>
    /// Gets the node's PostgreSQL installation.
    /// </summary>
    public PostgresInstallation Installation => Cluster.Installation;

    /// <summary>
    /// Gets the connection string for the node's server, without a database or user, as a TAP node's connstr.
    /// </summary>
    public string ConnStr => $"host=127.0.0.1 port={Cluster.Port.ToString(CultureInfo.InvariantCulture)}";

    /// <summary>
    /// Starts a node with worf in shared_preload_libraries and the given settings.
    /// </summary>
    /// <param name="name">The node's name.</param>
    /// <param name="configuration">The node's postgresql.conf settings.</param>
    /// <param name="cancellationToken">Cancels startup.</param>
    /// <param name="installation">The PostgreSQL installation, or null for the one the test run selects.</param>
    /// <returns>The node.</returns>
    public static async Task<TestNode> StartAsync(string name, IReadOnlyList<string> configuration,
        CancellationToken cancellationToken, PostgresInstallation? installation = null)
    {
        PostgresExtensionTestOptions options = installation is null
            ? new()
            {
                ProjectPath = RepositoryPaths.ExtensionProject,
                SharedPreload = true,
                PostgreSqlConfiguration = configuration,
            }
            : new()
            {
                ProjectPath = RepositoryPaths.ExtensionProject,
                SharedPreload = true,
                PostgreSqlConfiguration = configuration,
                Installation = installation,

                // An explicit installation builds against its own headers, in place of the one the test run selects.
                BuildProperties = new Dictionary<string, string>
                {
                    ["AnkusPgConfigPath"] = installation.PgConfigPath,
                    ["AnkusPostgresMajor"] = installation.Version.Major.ToString(CultureInfo.InvariantCulture),
                },
            };
        PostgresExtensionTest extension = await PostgresExtensionTest.StartAsync(options, cancellationToken);

        TestNode node = new(name, extension);
        try
        {
            await node.TrustOutputPluginAsync(cancellationToken);
        }
        catch
        {
            await node.DisposeAsync();
            throw;
        }

        return node;
    }

    /// <summary>
    /// Gets a connection string for a database on this node.
    /// </summary>
    /// <param name="dbname">The database.</param>
    /// <param name="user">The user, or null for the cluster's superuser.</param>
    /// <returns>The connection string.</returns>
    public string Dsn(string dbname, string? user = null)
        => user is null ? $"{ConnStr} dbname={dbname}" : $"{ConnStr} dbname={dbname} user={user}";

    /// <summary>
    /// Opens a connection to a database on this node, as the cluster's superuser or a role that logs in itself.
    /// </summary>
    /// <param name="dbname">The database.</param>
    /// <param name="cancellationToken">Cancels connecting.</param>
    /// <param name="user">The role.</param>
    /// <returns>The open connection.</returns>
    public async Task<NpgsqlConnection> OpenAsync(string dbname, CancellationToken cancellationToken, string user = "postgres")
    {
        NpgsqlConnectionStringBuilder builder = new()
        {
            Host = "127.0.0.1",
            Port = Cluster.Port,
            Database = dbname,
            Username = user,
            Pooling = false,
            Timeout = 60,
        };
        NpgsqlConnection connection = new(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    /// <summary>
    /// Runs SQL and returns its rows as psql's unaligned, tuples-only output does: columns joined by <c>|</c> and rows
    /// by newlines, as <c>safe_psql</c> returns them.
    /// </summary>
    /// <param name="dbname">The database.</param>
    /// <param name="sql">The SQL.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <param name="user">The role that runs it.</param>
    /// <returns>The output.</returns>
    public async Task<string> SafePsqlAsync(string dbname, string sql, CancellationToken cancellationToken, string user = "postgres")
    {
        await using NpgsqlConnection connection = await OpenAsync(dbname, cancellationToken, user);
        return await QueryAsync(connection, sql, cancellationToken);
    }

    /// <summary>
    /// Runs SQL on a session and returns its rows as <see cref="SafePsqlAsync"/> does.
    /// </summary>
    private static async Task<string> QueryAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using NpgsqlCommand command = new(sql, connection) { CommandTimeout = 0 };
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken);

        StringBuilder output = new();
        do
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                if (output.Length > 0)
                {
                    _ = output.Append('\n');
                }

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (i > 0)
                    {
                        _ = output.Append('|');
                    }

                    // A void result reads as null.
                    if (!reader.IsDBNull(i) && reader.GetValue(i) is { } value)
                    {
                        _ = output.Append(FormatValue(value, reader.GetDataTypeName(i)));
                    }
                }
            }
        }
        while (await reader.NextResultAsync(cancellationToken));

        return output.ToString();
    }

    /// <summary>
    /// Runs SQL and reports whether it succeeded, as <c>psql</c> and <c>command_fails</c> do.
    /// </summary>
    /// <param name="dbname">The database.</param>
    /// <param name="sql">The SQL.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <returns>Whether the SQL succeeded.</returns>
    public async Task<bool> PsqlAsync(string dbname, string sql, CancellationToken cancellationToken)
    {
        try
        {
            _ = await SafePsqlAsync(dbname, sql, cancellationToken);
            return true;
        }
        catch (PostgresException)
        {
            return false;
        }
    }

    /// <summary>
    /// Runs a query until it returns <c>t</c>, as <c>poll_query_until</c> does, on one session while it lasts rather
    /// than a session each time, which a server spawns a process for. A query that fails on the session, as once the
    /// server restarted, runs again on a new one, whose failure ends the poll as a query's always has.
    /// </summary>
    /// <param name="dbname">The database.</param>
    /// <param name="sql">The query.</param>
    /// <param name="cancellationToken">Cancels polling.</param>
    /// <returns>Whether the query returned <c>t</c> before the timeout.</returns>
    public async Task<bool> PollQueryUntilAsync(string dbname, string sql, CancellationToken cancellationToken)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        NpgsqlConnection? connection = null;
        try
        {
            while (elapsed.Elapsed < PollTimeout)
            {
                string result;
                try
                {
                    connection ??= await OpenAsync(dbname, cancellationToken);
                    result = await QueryAsync(connection, sql, cancellationToken);
                }
                catch (NpgsqlException) when (connection is not null && !cancellationToken.IsCancellationRequested)
                {
                    await connection.DisposeAsync();
                    connection = await OpenAsync(dbname, cancellationToken);
                    result = await QueryAsync(connection, sql, cancellationToken);
                }

                if (result == "t")
                {
                    return true;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
            }

            return false;
        }
        finally
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Polls the node's server log until it has the text after a position.
    /// </summary>
    /// <param name="logpos">The position in the log to look from.</param>
    /// <param name="text">The text.</param>
    /// <param name="cancellationToken">Cancels polling.</param>
    /// <returns>Whether the text appeared before the timeout.</returns>
    public async Task<bool> PollLogAsync(int logpos, string text, CancellationToken cancellationToken)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < PollTimeout)
        {
            if (ReadLog()[logpos..].Contains(text, StringComparison.Ordinal))
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }

        return false;
    }

    /// <summary>
    /// Runs one of the installation's programs against this node and waits for it to exit.
    /// </summary>
    /// <param name="program">The program's name, such as <c>pgbench</c>.</param>
    /// <param name="arguments">The program's arguments.</param>
    /// <param name="cancellationToken">Cancels the program.</param>
    /// <returns>The program's exit code and output.</returns>
    public async Task<(int ExitCode, string Output)> RunProgramAsync(string program, IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        (int exitCode, string stdout, string stderr) = await ProcessRunner.RunAsync(ProgramPath(program), arguments,
            cancellationToken, environment: ProgramEnvironment());
        return (exitCode, stdout + stderr);
    }

    /// <summary>
    /// Starts one of the installation's programs against this node.
    /// </summary>
    /// <param name="program">The program's name, such as <c>pgbench</c>.</param>
    /// <param name="arguments">The program's arguments.</param>
    /// <returns>The running program, with its output redirected.</returns>
    public Process StartProgram(string program, IReadOnlyList<string> arguments)
    {
        ProcessStartInfo startInfo = new(ProgramPath(program))
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach ((string name, string value) in ProgramEnvironment())
        {
            startInfo.Environment[name] = value;
        }

        foreach (string argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        return Process.Start(startInfo) ?? throw new InvalidOperationException($"could not start {program}");
    }

    /// <summary>
    /// Stops the node's server, as <c>pg_ctl stop</c> does.
    /// </summary>
    /// <param name="mode">The shutdown mode: <c>smart</c>, <c>fast</c> or <c>immediate</c>.</param>
    /// <param name="cancellationToken">Cancels waiting for the server.</param>
    /// <returns>The work.</returns>
    public Task StopAsync(string mode, CancellationToken cancellationToken)
        => RunPgCtlAsync(["stop", "-D", Cluster.DataDirectory, "-m", mode, "-w"], cancellationToken);

    /// <summary>
    /// Starts the node's server again after <see cref="StopAsync"/>, with the settings it had.
    /// </summary>
    /// <param name="cancellationToken">Cancels waiting for the server.</param>
    /// <param name="options">Options for the server's command line, such as <c>-c name=value</c>, or null.</param>
    /// <param name="environment">Environment variables the server gets besides this process's, or null.</param>
    /// <returns>The work.</returns>
    public Task StartServerAsync(CancellationToken cancellationToken, string? options = null,
        IReadOnlyDictionary<string, string>? environment = null)
        => RunPgCtlAsync(["start", "-D", Cluster.DataDirectory, "-l", RestartLogPath, "-w", "-t", "180",
            .. options is null ? Array.Empty<string>() : ["-o", options]], cancellationToken, environment);

    /// <summary>
    /// Reads the node's server log, and what the server wrote after <see cref="StartServerAsync"/>.
    /// </summary>
    /// <returns>The log.</returns>
    public string ReadLog()
    {
        string log = Cluster.ReadServerLog();
        if (!File.Exists(RestartLogPath))
        {
            return log;
        }

        // The server may still write it.
        using FileStream stream = new(RestartLogPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using StreamReader reader = new(stream);
        return log + reader.ReadToEnd();
    }

    /// <summary>
    /// Finds when the server logged each line of its log from a position on that holds a text, by the server's clock,
    /// which a test host busy with other tests may fall seconds behind in reading.
    /// </summary>
    /// <param name="from">The position in the log.</param>
    /// <param name="text">The text.</param>
    /// <returns>The times, in order.</returns>
    public List<DateTime> LogTimes(int from, string text)
        => [.. ReadLog()[from..].Split('\n')
            .Where(line => line.StartsWith('[') && line.Contains(text, StringComparison.Ordinal))
            .Select(line => DateTime.ParseExact(line[1..24], "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture))];

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _extension.DisposeAsync();

    // A server started again logs to a file beside the harness's: on Windows the cmd.exe that pg_ctl runs it under
    // holds the log until a moment after the server stops, when the harness replaces its own log.
    private string RestartLogPath => Cluster.LogFilePath + ".restart.log";

    // Values read as psql prints them: dates and timestamps in ISO form, a timestamp with time zone in UTC, so that SQL
    // reads one back as the same time whatever the session's time zone.
    private static string FormatValue(object value, string type) => value switch
    {
        bool b => b ? "t" : "f",
        DateTime at when type == "timestamp with time zone" => at.ToString("yyyy-MM-dd HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture) + "+00",
        DateTime at when type == "timestamp without time zone" => at.ToString("yyyy-MM-dd HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture),
        DateTime at when type == "date" => at.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    /// <summary>
    /// Lists worf in output_plugin_libraries, which current PostgreSQL releases require of every output plugin, as a
    /// DBA does. Earlier minor releases have no such setting.
    /// </summary>
    private async Task TrustOutputPluginAsync(CancellationToken cancellationToken)
    {
        const string Setting = "pgoutput, test_decoding, worf";
        const string Database = "postgres";
        if (await SafePsqlAsync(Database, "SELECT count(*) FROM pg_settings WHERE name = 'output_plugin_libraries'", cancellationToken) != "1")
        {
            return;
        }

        _ = await SafePsqlAsync(Database, $"ALTER SYSTEM SET output_plugin_libraries = {Setting}", cancellationToken);
        _ = await SafePsqlAsync(Database, "SELECT pg_reload_conf()", cancellationToken);
        if (!await PollQueryUntilAsync(Database, $"SELECT current_setting('output_plugin_libraries') = '{Setting}'", cancellationToken))
        {
            throw new InvalidOperationException("output_plugin_libraries did not take effect");
        }
    }

    private async Task RunPgCtlAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken,
        IReadOnlyDictionary<string, string>? environment = null)
    {
        (int exitCode, string stdout, string stderr) = await ProcessRunner.RunAsync(Installation.PgCtlPath, arguments,
            cancellationToken, environment: environment);
        if (exitCode != 0)
        {
            throw new InvalidOperationException($"pg_ctl {arguments[0]} failed: {stdout}{stderr}");
        }
    }

    private string ProgramPath(string program)
        => Path.Join(Installation.BinDirectory, OperatingSystem.IsWindows() ? program + ".exe" : program);

    private Dictionary<string, string> ProgramEnvironment() => new()
    {
        ["PGHOST"] = "127.0.0.1",
        ["PGPORT"] = Cluster.Port.ToString(CultureInfo.InvariantCulture),
        ["PGUSER"] = "postgres",
    };
}
