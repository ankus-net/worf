using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Worf.Tests;

/// <summary>
/// Port of pglogical's <c>t/040_pglogical_sync_during_write.pl</c>: tables resynchronize correctly while pgbench writes to
/// them.
/// </summary>
[TestClass]
public sealed partial class SyncDuringWriteTests
{
    private const string Database = "pgltest";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Resynchronizes each pgbench table in turn until pgbench finishes, then checks that the subscriber has every row.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ResyncWhilePgbenchRuns()
    {
        CancellationToken token = TestContext.CancellationToken;
        string pgbenchScale = Environment.GetEnvironmentVariable("PGBENCH_SCALE") ?? "1";
        string pgbenchClients = Environment.GetEnvironmentVariable("PGBENCH_CLIENTS") ?? "10";
        string pgbenchJobs = Environment.GetEnvironmentVariable("PGBENCH_JOBS") ?? "1";
        int pgbenchTime = int.Parse(Environment.GetEnvironmentVariable("PGBENCH_TIME") ?? "120", CultureInfo.InvariantCulture);
        string walsenderTimeout = Environment.GetEnvironmentVariable("PGBENCH_TIMEOUT") ?? "5s";

        await using TestNode provider = await TestNode.StartAsync("provider",
        [
            "wal_level = 'logical'",
            "max_replication_slots = 12",
            "max_wal_senders = 12",
            $"wal_sender_timeout = '{walsenderTimeout}'",
            "max_connections = 200",
            "log_line_prefix = '%t %p '",
            "track_commit_timestamp = on",
            "worf.synchronous_commit = true",
        ], token);
        _ = await provider.SafePsqlAsync("postgres", $"CREATE DATABASE {Database}", token);

        await using TestNode subscriber = await TestNode.StartAsync("subscriber",
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "fsync=off",
            "worf.synchronous_commit = true",
            "log_line_prefix = '%t %p '",
        ], token);
        _ = await subscriber.SafePsqlAsync("postgres", $"CREATE DATABASE {Database}", token);

        _ = await provider.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER;", token);
        _ = await provider.SafePsqlAsync(Database, "CREATE EXTENSION IF NOT EXISTS worf VERSION '1.0.0';", token);
        _ = await provider.SafePsqlAsync(Database, "ALTER EXTENSION worf UPDATE;", token);

        string providerConnstr = provider.Dsn(Database, SuperUser);
        TestContext.WriteLine($"node_provider - connstr : {provider.ConnStr}");
        _ = await provider.SafePsqlAsync(Database,
            $"SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := '{providerConnstr}');", token);

        _ = await subscriber.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER;", token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE EXTENSION IF NOT EXISTS worf VERSION '1.0.0';", token);
        _ = await subscriber.SafePsqlAsync(Database, "ALTER EXTENSION worf UPDATE;", token);

        TestContext.WriteLine($"node_subscriber - connstr : {subscriber.ConnStr}");
        _ = await subscriber.SafePsqlAsync(Database,
            $"SELECT * FROM worf.create_node(node_name := 'test_subscriber', dsn := '{subscriber.Dsn(Database, SuperUser)}');",
            token);

        (int initExit, string initOutput) = await provider.RunProgramAsync("pgbench", ["-i", "-s", pgbenchScale, Database], token);
        Assert.AreEqual(0, initExit, $"initialize pgbench: {initOutput}");

        string[] pgbenchTables = ["pgbench_accounts", "pgbench_tellers", "pgbench_history"];
        foreach (string tbl in pgbenchTables)
        {
            string setname = tbl == "pgbench_history" ? "default_insert_only" : "default";
            _ = await provider.SafePsqlAsync(Database,
                $"SELECT * FROM worf.replication_set_add_table('{setname}', '{tbl}', false);", token);
        }

        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'test_subscription',
                synchronize_structure := true,
                synchronize_data := true,
                provider_dsn := '{providerConnstr}'
            );
            """, token);

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.show_subscription_status() where subscription_name = 'test_subscription' AND status = 'replicating')",
            token), "subscription failed to reach \"replicating\" state");

        TestContext.WriteLine($"provider is {provider.Name}");
        TestContext.WriteLine($"max_connections is {await provider.SafePsqlAsync(Database, "SHOW max_connections;", token)}");

        using Process pgbench = provider.StartProgram("pgbench",
        [
            "-T", pgbenchTime.ToString(CultureInfo.InvariantCulture), "-j", pgbenchJobs, "-s", pgbenchScale, "-c", pgbenchClients,
            provider.Dsn(Database, "postgres"),
        ]);
        Task<string> pgbenchStdout = pgbench.StandardOutput.ReadToEndAsync(token);
        Task<string> pgbenchStderr = pgbench.StandardError.ReadToEndAsync(token);

        if (!await provider.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE query like 'UPDATE pgbench%')", token))
        {
            // Say why: pgbench may have failed to start its clients.
            string output = pgbench.HasExited ? $"exited with {pgbench.ExitCode}: {await pgbenchStdout}{await pgbenchStderr}" : "still running";
            Assert.Fail($"pgbench process is not running currently; pgbench {output}");
        }

        _ = await provider.SafePsqlAsync(Database, "ALTER SYSTEM SET log_statement = 'ddl'", token);
        _ = await provider.SafePsqlAsync(Database, "SELECT pg_reload_conf();", token);

        TestContext.WriteLine($"warming up pgbench for {pgbenchTime / 10}s");
        await Task.Delay(TimeSpan.FromSeconds(pgbenchTime / 10.0), token);
        TestContext.WriteLine("done warmup");

        int walsenderPid = ToPid(await provider.SafePsqlAsync(Database,
            "SELECT pid FROM pg_stat_activity WHERE application_name = 'test_subscription'", token));
        int applyPid = ToPid(await subscriber.SafePsqlAsync(Database,
            "SELECT pid FROM pg_stat_activity WHERE application_name LIKE '%apply%'", token));
        TestContext.WriteLine($"wal sender pid is {walsenderPid}; apply worker pid is {applyPid}");

        List<string> failures = [];
        int publogpos = provider.ReadLog().Length;
        int i = 1;
        do
        {
            // Resync all the tables in turn
            foreach (string tbl in pgbenchTables)
            {
                Stopwatch resyncStart = Stopwatch.StartNew();
                try
                {
                    _ = await subscriber.SafePsqlAsync(Database,
                        $"SELECT * FROM worf.alter_subscription_resynchronize_table('test_subscription', '{tbl}');", token);
                }
                catch (Npgsql.PostgresException error)
                {
                    string syncStatus = await subscriber.SafePsqlAsync(Database,
                        $"SELECT sync_status FROM worf.local_sync_status WHERE sync_relname = '{tbl}'", token);
                    TestContext.WriteLine($"attempt to resync {tbl} failed with {error.Message}; sync_status is currently {syncStatus}");
                    failures.Add($"{tbl} didn't sync: resync request failed");
                    continue;
                }

                while (true)
                {
                    await Task.Delay(TimeSpan.FromMicroseconds(100), token);
                    // Both are read in one query: read apart, a worker that starts between the two reads looks like
                    // one that has exited.
                    string[] state = (await subscriber.SafePsqlAsync(Database,
                        "SELECT (SELECT string_agg(pid::text, ',') FROM pg_stat_activity WHERE application_name LIKE '%sync%'), " +
                        $"(SELECT sync_status FROM worf.local_sync_status WHERE sync_relname = '{tbl}')", token)).Split('|');
                    string running = state[0];
                    string status = state.Length > 1 ? state[1] : "";
                    if (status == "r")
                    {
                        TestContext.WriteLine($"{tbl} synced on iteration {i} (elapsed {resyncStart.Elapsed.TotalSeconds})");
                        break;
                    }
                    else if (status is "i" or "y")
                    {
                        // 'i': worker still starting. 'y': worker done but master hasn't noticed yet; keep looping
                        // until master notices and switches to 'r'.
                    }
                    else if (running.Length == 0)
                    {
                        failures.Add($"{tbl} didn't sync on iteration {i}, sync worker exited (running={running}) while sync state was '{status}' (elapsed {resyncStart.Elapsed.TotalSeconds})");
                    }
                }

                // look for walsender timeouts in logs since last test
                string publog = provider.ReadLog();
                string? timeoutLine = publog[publogpos..].Split('\n').FirstOrDefault(line => line.Contains("replication timeout", StringComparison.Ordinal));
                publogpos = publog.Length;
                if (timeoutLine is not null)
                {
                    TestContext.WriteLine("status line after failed sync is " + await subscriber.SafePsqlAsync(Database,
                        $"SELECT * FROM worf.local_sync_status WHERE sync_relname = '{tbl}'", token));
                    Match match = LogLinePid().Match(timeoutLine);
                    if (match.Success)
                    {
                        TestContext.WriteLine(int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture) == walsenderPid
                            ? $"terminated walsender was the main walsender for the apply worker (apply pid {applyPid})"
                            : "terminated walsender was not for apply worker");
                    }
                    else
                    {
                        TestContext.WriteLine($"couldn't match line format for {timeoutLine}");
                    }

                    failures.Add($"walsender timeout since last test: {timeoutLine}");
                }
            }

            i++;

            // and repeat until pgbench exits
        }
        while (await provider.SafePsqlAsync(Database, "SELECT 1 FROM pg_stat_activity WHERE application_name = 'pgbench'", token) != "");

        await pgbench.WaitForExitAsync(token);
        TestContext.WriteLine("##### output of pgbench #####");
        TestContext.WriteLine(await pgbenchStdout);
        TestContext.WriteLine(await pgbenchStderr);
        TestContext.WriteLine("##### end of output #####");
        Assert.AreEqual(0, pgbench.ExitCode, "pgbench run successfull");

        TestContext.WriteLine(" waiting for catchup");
        _ = await provider.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL);", token);
        TestContext.WriteLine("comparing tables");
        foreach (string tbl in pgbenchTables)
        {
            string rowcountProvider = await provider.SafePsqlAsync(Database, $"SELECT count(*) FROM {tbl};", token);
            string rowcountSubscriber = await subscriber.SafePsqlAsync(Database, $"SELECT count(*) FROM {tbl};", token);
            if (rowcountSubscriber != rowcountProvider)
            {
                failures.Add($"final {tbl} row counts match after sync: provider has {rowcountProvider}, but subscriber has {rowcountSubscriber}");
            }
        }

        Assert.IsEmpty(failures, string.Join(Environment.NewLine, failures));
    }

    private static int ToPid(string value) => int.TryParse(value.Split('\n')[0], CultureInfo.InvariantCulture, out int pid) ? pid : 0;

    [GeneratedRegex(@"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [A-Z]+ (\d+) LOG:")]
    private static partial Regex LogLinePid();
}
