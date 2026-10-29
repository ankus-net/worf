using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;
using Npgsql;

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

        // pgbench writes as fast as it can unless given a rate in transactions a second, which bounds what the run takes
        // from the tests beside it.
        string? pgbenchRate = Environment.GetEnvironmentVariable("PGBENCH_RATE");
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

            // A walsender timeout means the apply stopped reading for that long; a lock it waited on shows here.
            "log_lock_waits = on",
        ], token);
        _ = await subscriber.SafePsqlAsync("postgres", $"CREATE DATABASE {Database}", token);

        _ = await provider.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER;", token);
        _ = await provider.SafePsqlAsync(Database, "CREATE EXTENSION IF NOT EXISTS worf VERSION '0.1.0';", token);
        _ = await provider.SafePsqlAsync(Database, "ALTER EXTENSION worf UPDATE;", token);

        string providerConnstr = provider.Dsn(Database, SuperUser);
        TestContext.WriteLine($"node_provider - connstr : {provider.ConnStr}");
        _ = await provider.SafePsqlAsync(Database,
            $"SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := '{providerConnstr}');", token);

        _ = await subscriber.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER;", token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE EXTENSION IF NOT EXISTS worf VERSION '0.1.0';", token);
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
            .. pgbenchRate is null ? Array.Empty<string>() : ["-R", pgbenchRate],
            provider.Dsn(Database, "postgres"),
        ]);
        Task<string> pgbenchStdout = ProcessRunner.ReadToEndAsync(pgbench.StandardOutput);
        Task<string> pgbenchStderr = ProcessRunner.ReadToEndAsync(pgbench.StandardError);

        // Polled as poll_query_until does, counting the polls: once a Windows run never saw pgbench's updates during
        // all 30 seconds of its writes, which only a few slow polls can explain.
        Stopwatch waiting = Stopwatch.StartNew();
        int polls = 0;
        TimeSpan longestPoll = TimeSpan.Zero;
        bool started = false;
        while (!started && waiting.Elapsed < TestNode.PollTimeout)
        {
            Stopwatch poll = Stopwatch.StartNew();
            started = await provider.SafePsqlAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE query like 'UPDATE pgbench%')", token) == "t";
            polls++;
            longestPoll = poll.Elapsed > longestPoll ? poll.Elapsed : longestPoll;
            if (!started)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }
        }

        TestContext.WriteLine($"polled {polls} times in {waiting.Elapsed.TotalSeconds:F1} seconds for pgbench's updates, the longest poll taking {longestPoll.TotalSeconds:F1}");
        if (!started)
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

        // What the workers and the walsender wait on, so that a walsender timeout shows what kept apply from replying.
        ConcurrentQueue<string> samples = new();
        using CancellationTokenSource samplerStop = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task sampler = SampleAsync(provider, subscriber, samples, samplerStop.Token);
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
                    // pglogical's test polls every 100 microseconds, which here would keep a processor busy with
                    // connections and queries that slow the sync it watches.
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
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
                    else if (running.Length == 0 && await SyncWorkerGoneAsync(subscriber, tbl, status, token))
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
                        string pid = match.Groups[1].Success ? match.Groups[1].Value : match.Groups[2].Value;
                        TestContext.WriteLine(int.Parse(pid, CultureInfo.InvariantCulture) == walsenderPid
                            ? $"terminated walsender was the main walsender for the apply worker (apply pid {applyPid})"
                            : "terminated walsender was not for apply worker");
                    }
                    else
                    {
                        TestContext.WriteLine($"couldn't match line format for {timeoutLine}");
                    }

                    failures.Add($"walsender timeout since last test: {timeoutLine}");
                    TestContext.WriteLine("##### waits before the timeout #####");
                    TestContext.WriteLine(string.Join(Environment.NewLine, samples));
                }
            }

            i++;

            // and repeat until pgbench exits
        }
        while (await provider.SafePsqlAsync(Database, "SELECT 1 FROM pg_stat_activity WHERE application_name = 'pgbench'", token) != "");

        await samplerStop.CancelAsync();
        await sampler;

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

    /// <summary>
    /// Samples, every half second until cancelled, the wait of each worker on the subscriber and of the walsender, with
    /// the positions apply reported last, keeping the last thirty seconds.
    /// </summary>
    private static async Task SampleAsync(TestNode provider, TestNode subscriber, ConcurrentQueue<string> samples, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                string workers = await subscriber.SafePsqlAsync(Database, """
                    SELECT string_agg(concat_ws(' ', pid, application_name, state, wait_event_type, wait_event, left(query, 40)), '; ')
                    FROM pg_stat_activity WHERE application_name ~ '(apply|sync|manager)'
                    """, token);
                string walsender = await provider.SafePsqlAsync(Database, """
                    SELECT concat_ws(' ', r.sent_lsn, r.write_lsn, r.flush_lsn, r.reply_time, a.wait_event_type, a.wait_event)
                    FROM pg_stat_replication r JOIN pg_stat_activity a ON a.pid = r.pid WHERE r.application_name = 'test_subscription'
                    """, token);
                samples.Enqueue($"{DateTime.UtcNow:HH:mm:ss.fff} subscriber: {workers} | walsender: {walsender}");
                if (samples.Count > 60)
                {
                    _ = samples.TryDequeue(out _);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (NpgsqlException)
            {
                // A sample that fails, as while the walsender restarts, is skipped.
            }
        }
    }

    /// <summary>
    /// Checks again whether a table's sync worker exited without moving its status on. The query that saw no worker
    /// read the status from its snapshot and the workers after it, so a worker that moved the status on and exited in
    /// between looks gone. A worker moves the status on before it exits, so reading the workers and then, in a later
    /// snapshot, the status, finds that status unchanged only when the worker died.
    /// </summary>
    private static async Task<bool> SyncWorkerGoneAsync(TestNode subscriber, string table, string status, CancellationToken token)
        => await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE '%sync%'", token) == "0" &&
            await subscriber.SafePsqlAsync(Database, $"SELECT sync_status FROM worf.local_sync_status WHERE sync_relname = '{table}'", token) == status;

    private static int ToPid(string value) => int.TryParse(value.Split('\n')[0], CultureInfo.InvariantCulture, out int pid) ? pid : 0;

    // The process ID in pglogical's '%t %p ' prefix, or in the '[%m] [%p] [%c] [%a]: ' one the test cluster logs with.
    [GeneratedRegex(@"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [A-Z]+ (\d+) LOG:|\] \[(\d+)\] \[[^\]]*\] \[[^\]]*\]: LOG:")]
    private static partial Regex LogLinePid();
}
