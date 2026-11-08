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

    // The publication of worf the test run's nodes share, made once.
    private static readonly SemaphoreSlim s_sharedLock = new(1, 1);
    private static SharedPublication? s_shared;

    private TestNode(string name, PostgresTestCluster cluster)
    {
        Name = name;
        Cluster = cluster;
    }

    /// <summary>
    /// Gets the node's name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the node's cluster.
    /// </summary>
    public PostgresTestCluster Cluster { get; }

    /// <summary>
    /// Gets the node's PostgreSQL installation.
    /// </summary>
    public PostgresInstallation Installation => Cluster.Installation;

    /// <summary>
    /// Gets the connection string for the node's server, without a database or user, as a TAP node's connstr.
    /// </summary>
    public string ConnStr => $"host=127.0.0.1 port={Cluster.Port.ToString(CultureInfo.InvariantCulture)}";

    /// <summary>
    /// Starts a node with worf in shared_preload_libraries and the given settings. The test run's nodes share one
    /// publication of worf, and before PostgreSQL 18, one staged copy of the installation the run selects with worf's
    /// files in it, which the harness would otherwise make for each node; each still has a cluster of its own, with worf
    /// created in its test database, as the harness creates it.
    /// </summary>
    /// <param name="name">The node's name.</param>
    /// <param name="configuration">The node's postgresql.conf settings.</param>
    /// <param name="cancellationToken">Cancels startup.</param>
    /// <param name="installation">A copy of the installation the test run selects to run the node from, into which
    /// <see cref="PublicationAsync"/>'s files are installed before PostgreSQL 18, or null for the shared one.</param>
    /// <param name="environment">Environment variables the node's server runs with, or null.</param>
    /// <returns>The node.</returns>
    public static async Task<TestNode> StartAsync(string name, IReadOnlyList<string> configuration,
        CancellationToken cancellationToken, PostgresInstallation? installation = null, IReadOnlyDictionary<string, string?>? environment = null)
    {
        SharedPublication shared = await SharedAsync(cancellationToken);

        // Clusters go where the harness puts its own, as ankus test --pgdata says, and their logs beside its own,
        // which outlast the run.
        string? dataDirectory = Environment.GetEnvironmentVariable("ANKUS_TEST_DATA_DIRECTORY");
        PostgresTestCluster cluster = await PostgresTestCluster.StartAsync(new PostgresTestClusterOptions
        {
            Installation = installation ?? shared.Installation,
            DataDirectoryBase = string.IsNullOrEmpty(dataDirectory)
                ? new PostgresTestClusterOptions { Installation = shared.Installation }.DataDirectoryBase
                : dataDirectory,
            LogDirectory = shared.LogDirectory,
            PostgreSqlConfiguration = [.. shared.Settings, .. configuration],
            ProcessEnvironment = environment is null ? new Dictionary<string, string?>() : new Dictionary<string, string?>(environment),
        }, cancellationToken);
        TestNode node = new(name, cluster);
        try
        {
            await using NpgsqlConnection connection = await cluster.OpenConnectionAsync(cancellationToken);
            _ = await QueryAsync(connection, "CREATE EXTENSION worf", cancellationToken);
        }
        catch
        {
            await node.DisposeAsync();
            throw;
        }

        return node;
    }

    /// <summary>
    /// Gets the directory of the publication of worf that the test run's nodes load, whose control and SQL files an
    /// installation copy has to have before PostgreSQL 18.
    /// </summary>
    /// <param name="cancellationToken">Cancels publishing.</param>
    /// <returns>The directory.</returns>
    public static async Task<string> PublicationAsync(CancellationToken cancellationToken)
        => (await SharedAsync(cancellationToken)).Directory;

    /// <summary>
    /// Deletes the publication and staged installation the test run's nodes shared, once they've all stopped.
    /// </summary>
    /// <returns>The work.</returns>
    public static async Task DisposeSharedAsync()
    {
        if (s_shared is { } shared)
        {
            s_shared = null;
            await shared.DisposeAsync();
        }
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
    /// than a session each time, which a server spawns a process for. A server that refuses the session or drops it, as
    /// while it starts or recovers or once it restarted, is polled again on a new one; any other failure ends the poll.
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
                try
                {
                    connection ??= await OpenAsync(dbname, cancellationToken);
                    if (await QueryAsync(connection, sql, cancellationToken) == "t")
                    {
                        return true;
                    }
                }
                catch (NpgsqlException error) when (!cancellationToken.IsCancellationRequested &&
                    (error is not PostgresException postgres || postgres.SqlState.StartsWith("57", StringComparison.Ordinal) ||
                        postgres.SqlState.StartsWith("08", StringComparison.Ordinal)))
                {
                    // A server that is still starting or recovering refuses the connection, and one that restarted drops
                    // it, so it's made again at the next poll; any other error fails at once.
                    if (connection is not null)
                    {
                        await connection.DisposeAsync();
                        connection = null;
                    }
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
    /// Describes the WAL each logical slot decodes again as it starts, from its restart point to the point it has
    /// confirmed, for a failure's message: the transaction records, and every record of each transaction that logged
    /// invalidations there, as pg_waldump shows them, which name the subtransaction logical decoding stopped at.
    /// </summary>
    /// <param name="dbname">The database.</param>
    /// <param name="cancellationToken">Cancels the queries.</param>
    /// <returns>The description.</returns>
    public async Task<string> DescribeDecodedWalAsync(string dbname, CancellationToken cancellationToken)
    {
        StringBuilder described = new();
        string slots = await SafePsqlAsync(dbname, """
            SELECT slot_name || '|' || restart_lsn || '|' || confirmed_flush_lsn || '|' || pg_walfile_name(restart_lsn) FROM pg_replication_slots
            WHERE slot_type = 'logical' AND restart_lsn IS NOT NULL AND confirmed_flush_lsn IS NOT NULL
            """, cancellationToken);
        foreach (string[] slot in slots.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(line => line.Split('|')))
        {
            // The start segment is named, as pg_waldump otherwise takes the segment size from the directory's first file,
            // which may be one the server has created but not yet written.
            (_, string output) = await RunProgramAsync("pg_waldump",
                ["-p", Path.Join(Cluster.DataDirectory, "pg_wal"), "-s", slot[1], "-e", slot[2], slot[3]], cancellationToken);
            string[] records = output.Split('\n');
            HashSet<string> invalidating = [.. records.Where(record => record.Contains("INVALIDATION", StringComparison.Ordinal))
                .Select(Transaction).Where(xid => xid is not ("" or "0"))];
            _ = described.Append(CultureInfo.InvariantCulture, $"slot {slot[0]}, {slot[1]} to {slot[2]}:\n");
            foreach (string record in records.Where(record => record.Contains("rmgr: Transaction", StringComparison.Ordinal) ||
                invalidating.Contains(Transaction(record))).Take(400))
            {
                _ = described.Append("  ").Append(record).Append('\n');
            }
        }

        return described.ToString();

        // The transaction a record names, as pg_waldump prints it after "tx:".
        static string Transaction(string record)
        {
            int at = record.IndexOf("tx:", StringComparison.Ordinal);
            return at < 0 ? "" : new string([.. record[(at + 3)..].TrimStart().TakeWhile(char.IsAsciiDigit)]);
        }
    }

    /// <summary>
    /// Describes the node's replication as it stands, for a failure's message: its sessions and what they wait on, the
    /// locks they wait for, slots, replication origins and worf's subscriptions, and outside Windows the server's
    /// processes, which show a worker that never connected. Each query gives up after a while, as one that waits on a
    /// lightweight lock can't be cancelled, and worf's status, which takes worf's own, comes last.
    /// </summary>
    /// <param name="dbname">The database.</param>
    /// <param name="cancellationToken">Cancels the queries.</param>
    /// <returns>The description.</returns>
    public async Task<string> DescribeReplicationAsync(string dbname, CancellationToken cancellationToken)
    {
        StringBuilder description = new($"{Name}:");
        foreach ((string what, string sql) in new[]
        {
            ("sessions", """
                SELECT pid, backend_type, state, wait_event_type, wait_event, xact_start, left(query, 100) FROM pg_catalog.pg_stat_activity
                WHERE pid <> pg_catalog.pg_backend_pid() ORDER BY pid
                """),
            ("waiting locks", "SELECT pid, locktype, relation::pg_catalog.regclass, transactionid, mode FROM pg_catalog.pg_locks WHERE NOT granted"),
            ("slots", "SELECT slot_name, active, active_pid, restart_lsn, confirmed_flush_lsn FROM pg_catalog.pg_replication_slots"),
            ("origins", "SELECT * FROM pg_catalog.pg_replication_origin_status"),
            ("subscriptions", "SELECT subscription_name, status, provider_node, slot_name, replication_sets::text, forward_origins::text FROM worf.show_subscription_status()"),
        })
        {
            string rows;
            using CancellationTokenSource patience = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            patience.CancelAfter(TimeSpan.FromSeconds(15));
            try
            {
                rows = await SafePsqlAsync(dbname, sql, patience.Token);
            }
            catch (Exception error) when (error is NpgsqlException or OperationCanceledException && !cancellationToken.IsCancellationRequested)
            {
                rows = error.Message;
            }

            _ = description.Append(CultureInfo.InvariantCulture, $"\n{what}:\n{rows}");
        }

        if (!OperatingSystem.IsWindows())
        {
            string postmaster = File.ReadLines(Path.Join(Cluster.DataDirectory, "postmaster.pid")).First().Trim();
            (_, string processes, _) = await ProcessRunner.RunAsync("ps", ["-A", "-o", "pid=,ppid=,stat=,etime=,command="], cancellationToken);
            _ = description.Append(CultureInfo.InvariantCulture, $"\nprocesses:\n{string.Join('\n', processes.Split('\n')
                .Where(line => line.Split(' ', StringSplitOptions.RemoveEmptyEntries) is [_, string parent, ..] && parent == postmaster))}");
        }

        return description.ToString();
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
    public ValueTask DisposeAsync() => Cluster.DisposeAsync();

    /// <summary>
    /// Gets the publication of worf the test run's nodes share, making it the first time.
    /// </summary>
    private static async Task<SharedPublication> SharedAsync(CancellationToken cancellationToken)
    {
        await s_sharedLock.WaitAsync(cancellationToken);
        try
        {
            return s_shared ??= await SharedPublication.StartAsync(cancellationToken);
        }
        finally
        {
            _ = s_sharedLock.Release();
        }
    }

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
