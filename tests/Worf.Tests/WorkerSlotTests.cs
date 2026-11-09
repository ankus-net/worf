using System.Diagnostics;
using System.Globalization;
using Npgsql;

namespace Worf.Tests;

/// <summary>
/// worf's workers take slots that outlive their processes while their database's manager may still read them.
/// </summary>
[TestClass]
public sealed class WorkerSlotTests
{
    private const string Database = "postgres";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Databases dropped with their workers running, as DROP DATABASE ... WITH (FORCE) drops them, more times than
    /// there are slots, leave no slots behind: a database created after them still gets its manager.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DroppedDatabasesFreeTheirWorkerSlots()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("node", ["max_worker_processes = 8"], token);
        for (int i = 0; i < 12; i++)
        {
            string database = $"dropped{i}";
            _ = await node.SafePsqlAsync(Database, $"CREATE DATABASE {database}", token);
            _ = await node.SafePsqlAsync(database, $"""
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := 'node{i}', dsn := '{node.Dsn(database)}');
                """, token);
            if (!await node.PollQueryUntilAsync(Database,
                $"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = '{database}' AND backend_type LIKE 'worf manager%')", token))
            {
                // The sessions and processes, as the supervisor's wait shows among them, say why no manager started.
                Assert.Fail($"{database}'s manager\n{await node.DescribeReplicationAsync(Database, token)}");
            }

            _ = await node.SafePsqlAsync(Database, $"DROP DATABASE {database} WITH (FORCE)", token);
        }

        _ = await node.SafePsqlAsync(Database, "CREATE DATABASE kept", token);
        _ = await node.SafePsqlAsync("kept", $"""
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'kept', dsn := '{node.Dsn("kept")}');
            """, token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = 'kept' AND backend_type LIKE 'worf manager%')", token),
            "the manager of the database created after them");
        Assert.DoesNotContain("all background worker slots are already used", node.ReadLog());
    }

    /// <summary>
    /// CREATE EXTENSION worf in a prepared transaction gives the database its manager once COMMIT PREPARED commits it,
    /// though the manager that PREPARE TRANSACTION woke found nothing to manage then, and though the server restarted
    /// in between, rather than at the supervisor's next look minutes later, and though the transaction also altered the
    /// database without changing which databases have a manager. Meanwhile the manager starts and exits once, rather
    /// than failing on the transaction's lock on the extension, whose relation is null, and starting again.
    /// </summary>
    /// <param name="restart">Whether the server restarts between PREPARE TRANSACTION and COMMIT PREPARED.</param>
    /// <param name="altersDatabase">Whether the transaction alters the database too.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow(false, false)]
    [DataRow(true, false)]
    [DataRow(false, true)]
    [DataRow(true, true)]
    public async Task PreparedExtensionsGetTheirManager(bool restart, bool altersDatabase)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("node", ["max_prepared_transactions = 2"], token);
        _ = await node.SafePsqlAsync(Database, "CREATE DATABASE prepared", token);
        const string Manager = "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = 'prepared' AND backend_type LIKE 'worf manager%')";
        _ = await node.SafePsqlAsync("prepared", $"""
            BEGIN;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'prepared', dsn := '{node.Dsn("prepared")}');
            {(altersDatabase ? "ALTER DATABASE prepared CONNECTION LIMIT 50;" : "")}
            PREPARE TRANSACTION 'extension';
            """, token);

        if (restart)
        {
            await node.StopAsync("fast", token);
            await node.StartServerAsync(token);
        }

        // The manager that the prepared transaction woke, or that the server started, finds nothing to manage yet, and
        // exits.
        int logpos = node.ReadLog().Length;
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT NOT ({Manager})", token), "no manager while the extension is prepared");
        string meanwhile = node.ReadLog()[logpos..];
        Assert.DoesNotContain("cannot be read as a non-nullable", meanwhile, "the managers' failures while the extension is prepared");
        Assert.IsLessThan(3, meanwhile.Split('\n').Count(line => line.Contains("starting background worker process \"worf manager", StringComparison.Ordinal)),
            "managers started in 3 s while the extension is prepared");

        _ = await node.SafePsqlAsync("prepared", "COMMIT PREPARED 'extension'", token);
        Stopwatch starting = Stopwatch.StartNew();
        while (await node.SafePsqlAsync(Database, Manager, token) != "t")
        {
            Assert.IsLessThan(TimeSpan.FromSeconds(30), starting.Elapsed, "the manager once COMMIT PREPARED committed the extension");
            await Task.Delay(TimeSpan.FromMilliseconds(200), token);
        }

        TestContext.WriteLine($"the manager after {starting.Elapsed.TotalSeconds:F1} s");
    }

    /// <summary>
    /// COMMIT PREPARED of a transaction that changed neither worf nor a database starts no manager, in its own database
    /// or any other, though the managers that found nothing to manage there exited, nor does it once the server
    /// restarted after the transaction was prepared: an insert, a read of pg_database, ALTER ROLE ... SET, GRANT
    /// CONNECT and COMMENT ON DATABASE, CREATE TABLE, a read or SELECT ... FOR UPDATE of worf.node, an explicit LOCK of
    /// worf.node, and CREATE TABLE and DROP TABLE, which writes worf's record of dropped columns, which don't keep the
    /// supervisor watching for their ends either.
    /// </summary>
    /// <param name="restart">Whether the server restarts between PREPARE TRANSACTION and COMMIT PREPARED.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task UnrelatedPreparedCommitsStartNoManager(bool restart)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("node", ["max_prepared_transactions = 40", "log_min_messages = debug1", "max_worker_processes = 32"], token);
        for (int i = 0; i < 3; i++)
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE DATABASE unrelated{i}", token);
        }

        _ = await node.SafePsqlAsync("unrelated0", "CREATE TABLE t (a int)", token);
        if (restart)
        {
            _ = await node.SafePsqlAsync("unrelated0", string.Concat(Enumerable.Range(0, 20).Select(i =>
                $"BEGIN; INSERT INTO t VALUES ({i}); PREPARE TRANSACTION 'restored{i}';")), token);
            _ = await node.SafePsqlAsync("unrelated0", "CREATE ROLE unrelated_role", token);
            _ = await node.SafePsqlAsync("unrelated0", """
                BEGIN; SELECT count(*) FROM pg_database; PREPARE TRANSACTION 'restored_read';
                BEGIN; ALTER ROLE unrelated_role SET work_mem = '8MB'; PREPARE TRANSACTION 'restored_role';
                BEGIN; GRANT CONNECT ON DATABASE unrelated1 TO unrelated_role; PREPARE TRANSACTION 'restored_grant';
                BEGIN; COMMENT ON DATABASE unrelated2 IS 'unrelated'; PREPARE TRANSACTION 'restored_comment';
                BEGIN; CREATE TABLE made (a int); PREPARE TRANSACTION 'restored_table';
                """, token);
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION IF NOT EXISTS worf", token);
            _ = await node.SafePsqlAsync(Database, """
                BEGIN; SELECT count(*) FROM worf.node; PREPARE TRANSACTION 'restored_node';
                BEGIN; SELECT * FROM worf.node FOR UPDATE; PREPARE TRANSACTION 'restored_node_locked';
                """ + string.Concat(Enumerable.Range(0, 3).Select(i =>
                $"BEGIN; CREATE TABLE dropped{i} (a int); DROP TABLE dropped{i}; PREPARE TRANSACTION 'restored_dropped{i}';")), token);
            foreach ((string database, string mode) in new[] { ("locked0", "ACCESS EXCLUSIVE"), ("locked1", "SHARE") })
            {
                _ = await node.SafePsqlAsync(Database, $"CREATE DATABASE {database}", token);
                _ = await node.SafePsqlAsync(database, "CREATE EXTENSION worf", token);
                _ = await node.SafePsqlAsync(database, $"BEGIN; LOCK worf.node IN {mode} MODE; PREPARE TRANSACTION 'restored_lock_{database}'", token);
            }

            await node.StopAsync("fast", token);
            await node.StartServerAsync(token);
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            int restored = Starts();

            // The supervisor waits for them without looking a few times a second.
            if (OperatingSystem.IsLinux())
            {
                string supervisor = await node.SafePsqlAsync(Database, "SELECT pid FROM pg_stat_activity WHERE backend_type = 'worf supervisor'", token);
                long Switches() => long.Parse(File.ReadLines($"/proc/{supervisor}/status").Single(line => line.StartsWith("voluntary_ctxt_switches:", StringComparison.Ordinal))
                    ["voluntary_ctxt_switches:".Length..].Trim(), CultureInfo.InvariantCulture);
                long switches = Switches();
                await Task.Delay(TimeSpan.FromSeconds(3), token);
                Assert.IsLessThan(10L, Switches() - switches, "the supervisor's waits in 3 s while the unrelated transactions stay prepared");
            }

            _ = await node.SafePsqlAsync("unrelated0", string.Concat(Enumerable.Range(0, 20).Select(i => $"COMMIT PREPARED 'restored{i}';")) + """
                COMMIT PREPARED 'restored_read'; COMMIT PREPARED 'restored_role'; COMMIT PREPARED 'restored_grant'; COMMIT PREPARED 'restored_comment';
                COMMIT PREPARED 'restored_table';
                """, token);
            _ = await node.SafePsqlAsync(Database, "COMMIT PREPARED 'restored_node'; COMMIT PREPARED 'restored_node_locked';" +
                string.Concat(Enumerable.Range(0, 3).Select(i => $"COMMIT PREPARED 'restored_dropped{i}';")), token);
            foreach (string database in new[] { "locked0", "locked1" })
            {
                _ = await node.SafePsqlAsync(database, $"COMMIT PREPARED 'restored_lock_{database}'", token);
            }

            await Task.Delay(TimeSpan.FromSeconds(3), token);
            Assert.AreEqual(restored, Starts(), "managers started by unrelated COMMIT PREPARED of transactions prepared before a restart");
            Assert.DoesNotContain("out of background worker slots", node.ReadLog(), "the node's log, as every database got its manager");

            // No manager took one of them for a transaction that may change worf or the databases, as locked1's, which
            // reads worf.node beside the SHARE lock, would have from its LOCK alone.
            Assert.DoesNotContain("prepared transactions that may change worf or the databases", node.ReadLog(), "the managers' scans of the restored transactions");
            return;
        }

        await Task.Delay(TimeSpan.FromSeconds(3), token);
        int before = Starts();

        // Nor does it read the catalogs, as one that scans pg_db_role_setting would.
        await using NpgsqlConnection committing = await node.OpenAsync("unrelated0", token);
        long scansBefore = await ScansAsync(committing, token);
        for (int i = 0; i < 20; i++)
        {
            await using NpgsqlCommand prepared = new($"BEGIN; INSERT INTO t VALUES ({i}); PREPARE TRANSACTION 'unrelated'; COMMIT PREPARED 'unrelated'", committing);
            _ = await prepared.ExecuteNonQueryAsync(token);
        }

        Assert.AreEqual(scansBefore, await ScansAsync(committing, token), "scans of pg_db_role_setting by 20 unrelated COMMIT PREPARED");
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.AreEqual(before, Starts(), "managers started by 20 unrelated COMMIT PREPARED");

        int Starts() => node.ReadLog().Split('\n').Count(line => line.Contains("starting background worker process \"worf manager", StringComparison.Ordinal));
    }

    /// <summary>
    /// COMMIT PREPARED of a transaction that changed a database, but not which databases have managers, as ALTER DATABASE
    /// ... CONNECTION LIMIT does, restored after a restart, starts no manager, though it commits after the supervisor's
    /// regular look, which the managers that found nothing to manage before don't hold back. The supervisor looks every
    /// 5 seconds here, rather than every 3 minutes.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task LateUnchangingCommitsStartNoManager()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("node",
            ["max_prepared_transactions = 4", "max_worker_processes = 32", "log_min_messages = debug1", "worf.supervisor_interval = 5000"], token);
        for (int i = 0; i < 4; i++)
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE DATABASE unchanged{i}", token);
        }

        _ = await node.SafePsqlAsync("unchanged0", "BEGIN; ALTER DATABASE unchanged1 CONNECTION LIMIT 50; PREPARE TRANSACTION 'limit'", token);
        await node.StopAsync("fast", token);
        await node.StartServerAsync(token);
        Stopwatch since = Stopwatch.StartNew();

        // The supervisor looks at once, and leaves the managers that found nothing alone for 5 seconds; the transaction
        // commits after that.
        await Task.Delay(TimeSpan.FromSeconds(15), token);
        int before = Starts();
        _ = await node.SafePsqlAsync("unchanged0", "COMMIT PREPARED 'limit'", token);
        await Task.Delay(TimeSpan.FromSeconds(10), token);
        TestContext.WriteLine($"committed {since.Elapsed.TotalSeconds:F0} s after the restart");
        Assert.AreEqual(before, Starts(), "managers started by a restored COMMIT PREPARED that changed no database's manager");

        int Starts() => node.ReadLog().Split('\n').Count(line => line.Contains("starting background worker process \"worf manager", StringComparison.Ordinal));
    }

    /// <summary>
    /// A database's manager exits once COMMIT PREPARED of a DROP EXTENSION worf has committed, as it does after a DROP
    /// EXTENSION committed at once, though the server restarted in between.
    /// </summary>
    /// <param name="restart">Whether the server restarts between PREPARE TRANSACTION and COMMIT PREPARED.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task PreparedDropsStopTheirManager(bool restart)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("node", ["max_prepared_transactions = 2"], token);
        _ = await node.SafePsqlAsync(Database, "CREATE DATABASE dropping", token);
        _ = await node.SafePsqlAsync("dropping", "CREATE EXTENSION worf", token);
        const string Manager = "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = 'dropping' AND backend_type LIKE 'worf manager%')";
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, Manager, token), "the manager of the database with worf");
        _ = await node.SafePsqlAsync("dropping", "BEGIN; DROP EXTENSION worf; PREPARE TRANSACTION 'dropped'", token);
        if (restart)
        {
            await node.StopAsync("fast", token);
            await node.StartServerAsync(token);
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, Manager, token), "the manager again, as worf is there until the drop commits");
        }

        _ = await node.SafePsqlAsync("dropping", "COMMIT PREPARED 'dropped'", token);
        Stopwatch exiting = Stopwatch.StartNew();
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT NOT ({Manager[7..]})", token), "the manager, once the drop committed");
        Assert.IsLessThan(TimeSpan.FromSeconds(30), exiting.Elapsed, "the manager's exit after the drop committed");
    }

    /// <summary>
    /// A subscription created in a prepared transaction gets its apply worker once COMMIT PREPARED commits it, as one
    /// committed at once does, rather than at the manager's next look, though the server restarted in between.
    /// </summary>
    /// <param name="restart">Whether the server restarts between PREPARE TRANSACTION and COMMIT PREPARED.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task PreparedSubscriptionsStartTheirWorker(bool restart)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await TestNode.StartAsync("provider", ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10"], token);
        await using TestNode subscriber = await TestNode.StartAsync("subscriber", ["max_prepared_transactions = 2"], token);
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := '{node.Name}', dsn := '{node.Dsn(Database, "postgres")}');
                """, token);
        }

        _ = await subscriber.SafePsqlAsync(Database, $"""
            BEGIN;
            SELECT worf.create_subscription(subscription_name := 'ps', provider_dsn := '{provider.Dsn(Database, "postgres")}',
                synchronize_structure := false, synchronize_data := false);
            PREPARE TRANSACTION 'ps';
            """, token);
        if (restart)
        {
            await subscriber.StopAsync("fast", token);
            await subscriber.StartServerAsync(token);
        }

        await Task.Delay(TimeSpan.FromSeconds(2), token);
        _ = await subscriber.SafePsqlAsync(Database, "COMMIT PREPARED 'ps'", token);
        Stopwatch starting = Stopwatch.StartNew();
        while (await subscriber.SafePsqlAsync(Database, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%')", token) != "t")
        {
            Assert.IsLessThan(TimeSpan.FromSeconds(1), starting.Elapsed, "the apply worker once COMMIT PREPARED committed the subscription");
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        }

        TestContext.WriteLine($"the apply worker after {starting.Elapsed.TotalSeconds:F2} s");
    }

    /// <summary>
    /// CREATE EXTENSION worf, in a transaction prepared on a primary after its standby started, which the standby
    /// replays, gives the database its manager once the promoted standby commits it, rather than at the supervisor's
    /// next look.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task PreparedOnThePrimaryGetsItsManagerOncePromoted()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode primary = await TestNode.StartAsync("primary", ["max_prepared_transactions = 2", "wal_level = logical"], token);
        _ = await primary.SafePsqlAsync(Database, "CREATE DATABASE prepared", token);
        using TemporaryDirectory work = new($"worf-promoted-{Guid.NewGuid():N}");
        string standby = Path.Join(work.FullName, "standby");
        string port = CreateSubscriberCommand.FreePort().ToString(CultureInfo.InvariantCulture);
        (int copied, string copyOutput) = await primary.RunProgramAsync("pg_basebackup",
            ["-D", standby, "-R", "-X", "stream", "-c", "fast", "-p", primary.Cluster.Port.ToString(CultureInfo.InvariantCulture)], token);
        Assert.AreEqual(0, copied, copyOutput);
        (int started, string startOutput) = await primary.RunProgramAsync("pg_ctl",
            ["start", "-w", "-D", standby, "-o", $"-p {port}", "-l", Path.Join(work.FullName, "standby.log")], token);
        Assert.AreEqual(0, started, startOutput);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            _ = await primary.SafePsqlAsync("prepared", $"""
                BEGIN;
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := 'prepared', dsn := '{primary.Dsn("prepared")}');
                PREPARE TRANSACTION 'extension';
                """, token);
            string prepared = await primary.SafePsqlAsync(Database, "SELECT pg_current_wal_lsn()", token);
            Stopwatch replaying = Stopwatch.StartNew();
            while (await StandbyAsync($"SELECT pg_last_wal_replay_lsn() >= '{prepared}'", Database) != "t")
            {
                Assert.IsLessThan(TimeSpan.FromSeconds(30), replaying.Elapsed, "the standby replaying the prepared transaction");
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }

            (int promoted, string promoteOutput) = await primary.RunProgramAsync("pg_ctl", ["promote", "-w", "-D", standby], token);
            Assert.AreEqual(0, promoted, promoteOutput);
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            _ = await StandbyAsync("COMMIT PREPARED 'extension'", "prepared");
            Stopwatch starting = Stopwatch.StartNew();
            while (await StandbyAsync("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = 'prepared' AND backend_type LIKE 'worf manager%')",
                Database) != "t")
            {
                Assert.IsLessThan(TimeSpan.FromSeconds(30), starting.Elapsed, "the manager once the promoted standby committed the extension");
                await Task.Delay(TimeSpan.FromMilliseconds(200), token);
            }

            TestContext.WriteLine($"the manager after {starting.Elapsed.TotalSeconds:F1} s");
        }
        finally
        {
            _ = await primary.RunProgramAsync("pg_ctl", ["stop", "-m", "immediate", "-D", standby], CancellationToken.None);
        }

        async Task<string> StandbyAsync(string sql, string database)
        {
            (int exitCode, string output) = await primary.RunProgramAsync("psql",
                ["-X", "-A", "-t", "-q", "-p", port, "-d", database, "-v", "ON_ERROR_STOP=1", "-c", sql], token);
            Assert.AreEqual(0, exitCode, output);
            return output.Trim();
        }
    }

    // How often pg_db_role_setting has been scanned whole, as the statistics show once the session's own are flushed.
    private static async Task<long> ScansAsync(NpgsqlConnection connection, CancellationToken token)
    {
        await using (NpgsqlCommand flush = new("SELECT pg_stat_force_next_flush(); SELECT pg_stat_clear_snapshot()", connection))
        {
            _ = await flush.ExecuteNonQueryAsync(token);
        }

        await using NpgsqlCommand scans = new("SELECT seq_scan FROM pg_stat_sys_tables WHERE relname = 'pg_db_role_setting'", connection);
        return (long)(await scans.ExecuteScalarAsync(token))!;
    }
}
