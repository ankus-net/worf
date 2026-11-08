using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Worf.Tests;

/// <summary>
/// Port of pglogical's <c>t/010_pglogical_create_subscriber.pl</c>.
/// </summary>
[TestClass]
public sealed class CreateSubscriberTests
{
    private const string Database = "postgres";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// The command prints its help, as <c>program_help_ok</c> checks.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ProgramHelp()
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-help-{Guid.NewGuid():N}");
        string command = await CreateSubscriberCommand.InstallAsync(await TestInstallations.CurrentAsync(token), Path.Join(work.FullName, "bin"));

        (int exitCode, string stdout, string stderr) = await CreateSubscriberCommand.RunAsync(command, ["--help"],
            Path.GetTempPath(), null, token);
        Assert.AreEqual(0, exitCode, "worf_create_subscriber --help exit code 0");
        Assert.AreNotEqual("", stdout.Trim(), "worf_create_subscriber --help goes to stdout");
        Assert.AreEqual("", stderr, "worf_create_subscriber --help nothing to stderr");
    }

    /// <summary>
    /// The command rejects an invalid option, as <c>program_options_handling_ok</c> checks.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ProgramOptionsHandling()
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-options-{Guid.NewGuid():N}");
        string command = await CreateSubscriberCommand.InstallAsync(await TestInstallations.CurrentAsync(token), Path.Join(work.FullName, "bin"));

        (int exitCode, _, string stderr) = await CreateSubscriberCommand.RunAsync(command, ["--not-a-valid-option"],
            Path.GetTempPath(), null, token);
        Assert.AreNotEqual(0, exitCode, "worf_create_subscriber with invalid option nonzero exit code");
        Assert.AreNotEqual("", stderr.Trim(), "worf_create_subscriber with invalid option prints error message");
    }

    /// <summary>
    /// The command turns a base backup of the provider into a subscriber that keeps the provider's data and receives
    /// its new changes, and rows the subscription applied take its later changes in order, rather than as conflicts.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task CreateSubscriberFromBaseBackup()
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-create-{Guid.NewGuid():N}");

        await using TestNode provider = await TestNode.StartAsync("provider",
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "DateStyle = 'ISO, DMY'",
            "log_line_prefix = '[%m] [%p] [%d] '",
            "log_statement = 'all'",
            "worf.synchronous_commit = true",
            "worf.sequence_range_check_interval = 100",
            "worf.sequence_range_exchange_interval = 500",

            // The subscriber takes the provider's settings, and keeps a row its subscription's changes conflict with.
            "worf.conflict_resolution = keep_local",
        ], token);
        int providerPort = provider.Cluster.Port;
        int pgport = CreateSubscriberCommand.FreePort();
        string providerDsn = $"postgresql://super@127.0.0.1:{providerPort.ToString(CultureInfo.InvariantCulture)}/postgres";
        string subscriberDsn = $"postgresql://super@127.0.0.1:{pgport.ToString(CultureInfo.InvariantCulture)}/postgres";
        string backupDir = Path.Join(work.FullName, "backupdir");

        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));

        string[] setup =
        [
            "CREATE USER super SUPERUSER",
            "CREATE SEQUENCE some_local_seq",
            "CREATE TABLE some_local_tbl(id serial primary key, key text unique not null, data text)",
            "INSERT INTO some_local_tbl(key, data) VALUES('key1', 'data1')",
            "INSERT INTO some_local_tbl(key, data) VALUES('key2', NULL)",
            "INSERT INTO some_local_tbl(key, data) VALUES('key3', 'data3')",
            "CREATE TABLE some_local_tbl1(id serial, key text unique not null, data text)",
            "INSERT INTO some_local_tbl1(key, data) VALUES('key1', 'data1')",
            "INSERT INTO some_local_tbl1(key, data) VALUES('key2', NULL)",
            "INSERT INTO some_local_tbl1(key, data) VALUES('key3', 'data3')",
            "CREATE TABLE some_local_tbl2(id serial, key text, data text)",
            "INSERT INTO some_local_tbl2(key, data) VALUES('key1', 'data1')",
            "INSERT INTO some_local_tbl2(key, data) VALUES('key2', NULL)",
            "INSERT INTO some_local_tbl2(key, data) VALUES('key3', 'data3')",
            "CREATE TABLE some_local_tbl3(id integer, key text, data text)",
            "INSERT INTO some_local_tbl3(key, data) VALUES('key1', 'data1')",
            "INSERT INTO some_local_tbl3(key, data) VALUES('key2', NULL)",
            "INSERT INTO some_local_tbl3(key, data) VALUES('key3', 'data3')",
            "CREATE EXTENSION IF NOT EXISTS worf VERSION '0.1.0'",
            "ALTER EXTENSION worf UPDATE",
            "SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super')",
            "CREATE TABLE ranged (id bigserial PRIMARY KEY, v text)",
            "SELECT worf.enable_sequence_range('ranged_id_seq', participants := '{}', range_size := 100)",
            "INSERT INTO ranged (v) SELECT 'before' FROM generate_series(1, 10)",
        ];
        foreach (string sql in setup)
        {
            _ = await provider.SafePsqlAsync(Database, sql, token);
        }

        // The Perl test starts the provider with its port on the command line, so the subscriber's base backup takes
        // its port from PGPORT. The test cluster keeps its port in postgresql.auto.conf, which the backup copies, so
        // the copy there goes.
        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)),
            token);

        // pg_basebackup asks for a spread checkpoint, which writes the provider's dirty buffers a few at a time for
        // minutes; writing them now leaves it nothing to spread.
        _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);

        try
        {
            (int exitCode, string stdout, string stderr) = await CreateSubscriberCommand.RunAsync(command,
            [
                "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn={subscriberDsn}",
                $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
                $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
                $"--postgresql-conf={Path.Join(provider.Cluster.DataDirectory, "postgresql.conf")}",
            ], work.FullName, new Dictionary<string, string> { ["PGPORT"] = pgport.ToString(CultureInfo.InvariantCulture) }, token);
            TestContext.WriteLine(stdout);
            TestContext.WriteLine(stderr);
            Assert.AreEqual(0, exitCode, "worf_create_subscriber check");

            await AssertPsqlOkAsync(provider, pgport, ["-c", "\\d"], "preseed check 1 ", token);
            await AssertPsqlOkAsync(provider, pgport, ["-c", "SELECT * from some_local_tbl"], "preseed check 2 ", token);

            (int basicExit, string basicOutput) = await provider.RunProgramAsync("psql",
                ["-X", "-d", Database, "-f", Path.Join(AppContext.BaseDirectory, "TestFiles", "basic.sql")], token);
            TestContext.WriteLine(basicOutput);
            Assert.AreEqual(0, basicExit, "basic.sql");

            // allow script to complete executing
            await Task.Delay(TimeSpan.FromSeconds(11), token);

            // check whether it is replicated
            await AssertPsqlOkAsync(provider, pgport, ["-c", "\\d"], "replication check 1 ", token);
            await AssertPsqlOkAsync(provider, pgport, ["-c", "SELECT * from basic_dml"], "replication check 2 ", token);
            await AssertPsqlOkAsync(provider, pgport, ["-c", "SELECT * from public.basic_dml"], "replication check 3 ", token);

            // The clone gave up the provider's blocks of a managed sequence before it took a node id of its own, and its
            // copied rows reach into the root, so it is refused until the sequence moves to a new epoch, whose root
            // starts above them. It then draws from blocks of its own, never sharing a key with the provider.
            await PollSubscriberAsync(provider, pgport,
                "SELECT state = 'refused' FROM worf.sequence_ranges WHERE sequence = 'ranged_id_seq'::regclass", "the clone refused", token);
            (int refusedExit, string refused) = await provider.RunProgramAsync("psql",
                ["-X", "-p", pgport.ToString(CultureInfo.InvariantCulture), "-d", Database, "-c", "INSERT INTO ranged (v) VALUES ('clone')"], token);
            Assert.AreNotEqual(0, refusedExit, refused);
            _ = await provider.SafePsqlAsync(Database, """
                SELECT worf.disable_sequence_range('ranged_id_seq');
                SELECT worf.enable_sequence_range('ranged_id_seq', participants := '{test_subscriber}', range_size := 100);
                """, token);
            await PollSubscriberAsync(provider, pgport, """
                SELECT r.state = 'active' AND r.epoch = 2 AND b.node_id = l.node_id
                FROM worf.sequence_ranges r
                    JOIN worf.sequence_range_block b ON b.seqoid = r.sequence AND b.state = 'active' AND b.lo = r.active_lo
                    CROSS JOIN worf.local_node l
                WHERE r.sequence = 'ranged_id_seq'::regclass
                """, "the clone drawing from a block of its own", token);

            _ = await SubscriberAsync(provider, pgport, "INSERT INTO ranged (v) SELECT 'clone' FROM generate_series(1, 20)", token);
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO ranged (v) SELECT 'provider' FROM generate_series(1, 20)", token);
            HashSet<string> keys = [.. (await provider.SafePsqlAsync(Database, "SELECT id FROM ranged WHERE v = 'provider'", token)).Split('\n')];
            foreach (string key in (await SubscriberAsync(provider, pgport, "SELECT id FROM ranged WHERE v = 'clone'", token)).Split('\n'))
            {
                Assert.DoesNotContain(key, keys, "a key the clone shares with the provider");
            }

            // The tool creates the subscription's origin without worf.
            _ = await provider.SafePsqlAsync(Database, "UPDATE basic_dml SET data = 'updated' WHERE other = 5", token);
            Stopwatch elapsed = Stopwatch.StartNew();
            string updated = "";
            while (updated != "updated" && elapsed.Elapsed < TimeSpan.FromSeconds(60))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                (_, updated) = await provider.RunProgramAsync("psql",
                    ["-X", "-At", "-p", pgport.ToString(CultureInfo.InvariantCulture), "-d", Database, "-c", "SELECT data FROM basic_dml WHERE other = 5"],
                    token);
                updated = updated.Trim();
            }

            Assert.AreEqual("updated", updated, "the update of a row the subscription applied");
        }
        finally
        {
            await StopAsync(provider, backupDir);
        }
    }

    /// <summary>
    /// A subscriber whose subscription fails to commit leaves no progress for its origin's id, which the rollback frees,
    /// so the next origin created with that id, as a PostgreSQL subscription's, starts from nothing, whether its update
    /// fails at once or at the commit, by a deferred trigger.
    /// </summary>
    /// <param name="deferred">Whether the trigger that refuses the update is deferred to the commit.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task FailedSubscriberLeavesNoOriginProgress(bool deferred)
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-failed-{Guid.NewGuid():N}");
        await using TestNode provider = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on"], token);
        int pgport = CreateSubscriberCommand.FreePort();
        string port = pgport.ToString(CultureInfo.InvariantCulture);
        string providerDsn = $"postgresql://super@127.0.0.1:{provider.Cluster.Port.ToString(CultureInfo.InvariantCulture)}/postgres";
        string backupDir = Path.Join(work.FullName, "backupdir");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));

        // The copy's CREATE EXTENSION worf gives its sync status a trigger that refuses the tool's update of it, in the
        // transaction that created and advanced the origin.
        string trigger = deferred
            ? "CREATE CONSTRAINT TRIGGER refuse AFTER UPDATE ON worf.local_sync_status DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION refuse()"
            : "CREATE TRIGGER refuse BEFORE UPDATE ON worf.local_sync_status EXECUTE FUNCTION refuse()";
        _ = await provider.SafePsqlAsync(Database, $$"""
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            CREATE FUNCTION refuse() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'refused'; END$$;
            CREATE FUNCTION add_refusal() RETURNS event_trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF to_regclass('worf.local_sync_status') IS NOT NULL THEN
                    {{trigger}};
                END IF;
            END$$;
            CREATE EVENT TRIGGER add_refusal ON ddl_command_end WHEN TAG IN ('CREATE EXTENSION') EXECUTE FUNCTION add_refusal();
            """, token);
        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)),
            token);
        _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);

        try
        {
            (int exitCode, string stdout, string stderr) = await CreateSubscriberCommand.RunAsync(command,
            [
                "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
                $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
                $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
                $"--postgresql-conf={Path.Join(provider.Cluster.DataDirectory, "postgresql.conf")}",
            ], work.FullName, new Dictionary<string, string> { ["PGPORT"] = port }, token);
            TestContext.WriteLine(stdout);
            TestContext.WriteLine(stderr);
            Assert.AreNotEqual(0, exitCode, "worf_create_subscriber with its update refused");
            Assert.Contains("refused", stderr);

            (int started, string startOutput) = await provider.RunProgramAsync("pg_ctl",
                ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", Path.Join(work.FullName, "subscriber.log")], token);
            Assert.AreEqual(0, started, startOutput);
            Assert.AreEqual("1|true", await SubscriberAsync(provider, pgport, """
                SELECT pg_catalog.pg_replication_origin_create('later')::text || '|' ||
                    (coalesce(pg_catalog.pg_replication_origin_progress('later', false), '0/0') = '0/0')::text
                """, token), "the id of the origin created next, and that it has no progress");
        }
        finally
        {
            await StopAsync(provider, backupDir);
        }
    }

    /// <summary>
    /// A subscriber stopped, as with SIGTERM, while its subscription waits for a provider that doesn't answer, cancels
    /// the wait and cleans up as on a failure, stopping the server it started, a second stop, while it does, waiting for
    /// that, and one killed leaves its transaction to roll back: none leaves progress for its origin's id. A run again on
    /// the data directory the killed one left with its server running fails, leaving the server as it was, and so does
    /// one once the server is stopped, at once, as the killed run promoted the data directory past the provider's
    /// timeline. Processes are stopped with signals, which Windows lacks.
    /// </summary>
    /// <param name="signals">The signals the tool gets, 40 ms apart.</param>
    /// <param name="exitCode">The tool's exit code.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    [DataRow("TERM", 1)]
    [DataRow("TERM TERM", 1)]
    [DataRow("KILL", 137)]
    public async Task StoppedSubscriberLeavesNoOriginProgress(string signals, int exitCode)
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-stopped-{Guid.NewGuid():N}");
        await using TestNode provider = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on"], token);
        int pgport = CreateSubscriberCommand.FreePort();
        string port = pgport.ToString(CultureInfo.InvariantCulture);
        string providerDsn = $"postgresql://super@127.0.0.1:{provider.Cluster.Port.ToString(CultureInfo.InvariantCulture)}/postgres";
        string backupDir = Path.Join(work.FullName, "backupdir");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));
        _ = await provider.SafePsqlAsync(Database, """
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            """, token);
        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)),
            token);
        _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
        string postmaster = File.ReadLines(Path.Join(provider.Cluster.DataDirectory, "postmaster.pid")).First().Trim();

        ProcessStartInfo start = new(command)
        {
            WorkingDirectory = work.FullName,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (string argument in new[]
        {
            "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
            $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
            $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
            $"--postgresql-conf={Path.Join(provider.Cluster.DataDirectory, "postgresql.conf")}",
        })
        {
            start.ArgumentList.Add(argument);
        }

        start.Environment["PGPORT"] = port;
        string[] arguments = [.. start.ArgumentList];
        bool stopped = false;
        try
        {
            using Process tool = Process.Start(start)!;
            Task<string> errors = tool.StandardError.ReadToEndAsync(token);
            string? line;
            do
            {
                line = await tool.StandardOutput.ReadLineAsync(token);
            }
            while (line is not null && !line.StartsWith("Initializing worf on the subscriber node:", StringComparison.Ordinal));

            Assert.IsNotNull(line, "the tool starting the subscriber with worf");

            // The provider stops answering new connections before the tool, which next reaches it in create_subscription,
            // makes one, and the tool is stopped once its backend waits to connect there, in the transaction that made the
            // origin, as libpq's connection wait, or worf's own, which PostgreSQL 15 shows as an extension's, shows.
            (int paused, _, string error) = await ProcessRunner.RunAsync("kill", ["-STOP", postmaster], token);
            Assert.AreEqual(0, paused, error);
            stopped = true;
            Stopwatch connecting = Stopwatch.StartNew();
            while (!tool.HasExited && (!File.Exists(Path.Join(backupDir, "postmaster.pid")) || await SubscriberAsync(provider, pgport, """
                SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity
                    WHERE state = 'active' AND query LIKE '%worf.create_subscription%'
                        AND (lower(wait_event) = 'libpqwalreceiverconnect' OR wait_event_type = 'Extension'))
                """, token, true) != "t"))
            {
                Assert.IsLessThan(TestNode.PollTimeout, connecting.Elapsed, "the tool's backend waiting to connect to the provider");
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }

            Assert.IsFalse(tool.HasExited, $"the tool, which exited before its backend waited to connect to the provider: {await errors}");
            string[] sent = signals.Split(' ');
            string toolPid = tool.Id.ToString(CultureInfo.InvariantCulture);

            // A second stop lands inside the cleanup: the subscriber's checkpointer is stopped, so that the pg_ctl stop
            // the cleanup runs waits for its shutdown checkpoint, and the tool, stopped again then, waits it out.
            string? checkpointer = null;
            if (sent.Length > 1)
            {
                string subscriberPostmaster = File.ReadLines(Path.Join(backupDir, "postmaster.pid")).First().Trim();
                checkpointer = (await ChildrenAsync(subscriberPostmaster, "checkpointer", token)).Single();
                (int held, _, string holdError) = await ProcessRunner.RunAsync("kill", ["-STOP", checkpointer], token);
                Assert.AreEqual(0, held, holdError);
            }

            try
            {
                (int signalled, _, error) = await ProcessRunner.RunAsync("kill", [$"-{sent[0]}", toolPid], token);
                Assert.AreEqual(0, signalled, error);
                if (checkpointer is not null)
                {
                    Stopwatch cleaning = Stopwatch.StartNew();
                    while ((await ChildrenAsync(toolPid, "pg_ctl", token)).Count == 0)
                    {
                        Assert.IsLessThan(TimeSpan.FromSeconds(30), cleaning.Elapsed, "the tool's pg_ctl stop");
                        await Task.Delay(TimeSpan.FromMilliseconds(50), token);
                    }

                    (signalled, _, error) = await ProcessRunner.RunAsync("kill", [$"-{sent[1]}", toolPid], token);
                    Assert.AreEqual(0, signalled, error);
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                    Assert.IsFalse(tool.HasExited, "the tool, stopped again as it cleans up");
                }
            }
            finally
            {
                if (checkpointer is not null)
                {
                    _ = await ProcessRunner.RunAsync("kill", ["-CONT", checkpointer], CancellationToken.None);
                }
            }

            using CancellationTokenSource exiting = CancellationTokenSource.CreateLinkedTokenSource(token);
            exiting.CancelAfter(TimeSpan.FromSeconds(60));
            await tool.WaitForExitAsync(exiting.Token);
            TestContext.WriteLine(await errors);
            Assert.AreEqual(exitCode, tool.ExitCode, "the stopped tool's exit code");
        }
        finally
        {
            if (stopped)
            {
                _ = await ProcessRunner.RunAsync("kill", ["-CONT", postmaster], CancellationToken.None);
            }
        }

        string rerun = exitCode == 1 ? "" : "1|True|0|f, 1|True|True|False|1";
        string? killedProgress = null;
        const string Progress = """
            SELECT count(*) FROM pg_catalog.pg_replication_origin_status WHERE external_id IS NULL AND coalesce(remote_lsn, '0/0') <> '0/0'
            """;
        try
        {
            (int status, string statusOutput) = await provider.RunProgramAsync("pg_ctl", ["status", "-D", backupDir], token);
            if (exitCode == 1)
            {
                Assert.AreEqual(3, status, $"the server the stopped tool started, which it stops: {statusOutput}");
            }
            else
            {
                // The killed tool's transaction ends once its backend finds the tool gone, and a run again fails at once.
                Assert.AreEqual(0, status, $"the server the killed tool started: {statusOutput}");
                await PollSubscriberAsync(provider, pgport,
                    "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE backend_type = 'client backend' AND pid <> pg_catalog.pg_backend_pid())",
                    "the killed tool's backend ending", token);

                // The origins' progress is read while the server runs, as shared memory holds it, which a stop that skips
                // the shutdown checkpoint would lose unless the WAL that carries it had reached disk.
                killedProgress = await SubscriberAsync(provider, pgport, Progress, token);
                (int again, _, string againErr) = await CreateSubscriberCommand.RunAsync(command, arguments, work.FullName,
                    new Dictionary<string, string> { ["PGPORT"] = port }, token);
                (int running, _) = await provider.RunProgramAsync("pg_ctl", ["status", "-D", backupDir], token);
                rerun = $"{again}|{againErr.Contains("A server is running in data directory", StringComparison.Ordinal)}|{running}";
                if (running == 0)
                {
                    rerun += $"|{await SubscriberAsync(provider, pgport, "SELECT pg_catalog.pg_is_in_recovery()", token)}";
                }

                // Once that server is stopped, a run again fails at once too, as the data directory the killed run
                // promoted is on a timeline past the provider's, and leaves the data directory as it was.
                await StopAsync(provider, backupDir);
                Stopwatch refusing = Stopwatch.StartNew();
                (int stoppedAgain, _, string stoppedErr) = await CreateSubscriberCommand.RunAsync(command, arguments, work.FullName,
                    new Dictionary<string, string> { ["PGPORT"] = port }, token);
                rerun += $", {stoppedAgain}|{stoppedErr.Contains("past the provider's timeline", StringComparison.Ordinal)}|{refusing.Elapsed < TimeSpan.FromSeconds(60)}|" +
                    $"{File.Exists(Path.Join(backupDir, "standby.signal"))}|" +
                    $"{File.ReadAllText(Path.Join(backupDir, "postgresql.auto.conf")).Split("recovery_target_name").Length - 1}";
            }

            await StopAsync(provider, backupDir);
            (int started, string startOutput) = await provider.RunProgramAsync("pg_ctl",
                ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", Path.Join(work.FullName, "subscriber.log")], token);
            Assert.AreEqual(0, started, startOutput);
            string progress = killedProgress ?? await SubscriberAsync(provider, pgport, Progress, token);
            Assert.AreEqual($"0, {(exitCode == 1 ? "" : "1|True|0|f, 1|True|True|False|1")}", $"{progress}, {rerun}",
                "the freed origin ids with progress, which an origin created next would start from, and after a kill, a run again's exit code, message, " +
                "and the server it leaves running, out of recovery, and once that is stopped, a run again's exit code, message, whether it failed at once, " +
                "and the standby.signal and recovery settings it leaves");
        }
        finally
        {
            await StopAsync(provider, backupDir);
        }
    }

    /// <summary>
    /// A subscriber stopped with SIGTERM before a subscription commits leaves its data directory as it was: stopped as it
    /// connects to a provider that doesn't answer, it ends at once; stopped as pg_basebackup makes the data directory, it
    /// removes what the backup wrote; stopped as the server it started catches up to the restore point, it stops the
    /// server and takes its recovery settings, the template's too, back out, and keeps a standby.signal the data directory
    /// had. A data directory that has run as a server of its own, out of recovery, is refused at once. Once a database's
    /// subscription has committed, a stop waits for the run to set the other databases up. Processes are stopped with
    /// signals, which Windows lacks.
    /// </summary>
    /// <param name="when">Where the run is stopped.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    [DataRow("connect")]
    [DataRow("basebackup")]
    [DataRow("catchup")]
    [DataRow("standby")]
    [DataRow("databases")]
    public async Task StoppedRunsLeaveTheirDataDirectoryAsItWas(string when)
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-stop-{when}-{Guid.NewGuid():N}");
        await using TestNode provider = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on"], token);
        int pgport = CreateSubscriberCommand.FreePort();
        string port = pgport.ToString(CultureInfo.InvariantCulture);
        string providerPort = provider.Cluster.Port.ToString(CultureInfo.InvariantCulture);
        string providerDsn = $"postgresql://super@127.0.0.1:{providerPort}/postgres";
        string backupDir = Path.Join(work.FullName, "backupdir");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));
        _ = await provider.SafePsqlAsync(Database, """
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            """, token);
        if (when == "databases")
        {
            _ = await provider.SafePsqlAsync(Database, "CREATE DATABASE db2", token);
            _ = await provider.SafePsqlAsync("db2", """
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := 'test_provider_db2', dsn := 'dbname=db2 user=super');
                """, token);
        }

        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)),
            token);
        _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
        string postmaster = File.ReadLines(Path.Join(provider.Cluster.DataDirectory, "postmaster.pid")).First().Trim();

        List<string> arguments =
        [
            "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
            $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
            $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
            $"--postgresql-conf={Path.Join(provider.Cluster.DataDirectory, "postgresql.conf")}",
        ];

        // The settings the data directory starts with: the provider's, which pg_basebackup copies, or those its own base
        // backup, made as a standby's, adds. The server that catches up streams from a port nothing listens on, through
        // the recovery template, so that it never reaches the restore point.
        string before = await File.ReadAllTextAsync(autoConf, token);
        string template = Path.Join(work.FullName, "recovery.conf");
        await File.WriteAllTextAsync(template,
            $"# recovery template\nprimary_conninfo = 'host=127.0.0.1 port={CreateSubscriberCommand.FreePort().ToString(CultureInfo.InvariantCulture)} user=super'\n", token);
        switch (when)
        {
            case "basebackup":
                arguments.Add("--extra-basebackup-args=--max-rate=32k");
                break;
            case "catchup":
                arguments.Add($"--recovery-conf={template}");
                break;
            case "standby":
                (int backedUp, string backupOutput) = await provider.RunProgramAsync("pg_basebackup",
                    ["-D", backupDir, "-d", providerDsn, "-X", "stream", "-R"], token);
                Assert.AreEqual(0, backedUp, backupOutput);
                before = await File.ReadAllTextAsync(Path.Join(backupDir, "postgresql.auto.conf"), token);
                arguments.Add($"--recovery-conf={template}");
                break;
            case "databases":
                arguments.Add("--databases=postgres,db2");
                break;
            default:
                break;
        }

        ProcessStartInfo start = new(command) { WorkingDirectory = work.FullName, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        start.Environment["PGPORT"] = port;
        bool paused = false;
        try
        {
            if (when == "connect")
            {
                (int stopping, _, string stopError) = await ProcessRunner.RunAsync("kill", ["-STOP", postmaster], token);
                Assert.AreEqual(0, stopping, stopError);
                paused = true;
            }

            using Process tool = Process.Start(start)!;
            Task<string> errors = tool.StandardError.ReadToEndAsync(token);
            StringBuilder seen = new();
            async Task SeeAsync(string text)
            {
                char[] buffer = new char[4096];
                while (!seen.ToString().Contains(text, StringComparison.Ordinal))
                {
                    int read = await tool.StandardOutput.ReadAsync(buffer, token);
                    Assert.AreNotEqual(0, read, $"the tool's output, which ended before \"{text}\": {seen}{await errors}");
                    _ = seen.Append(buffer, 0, read);
                }
            }

            switch (when)
            {
                case "connect":
                    await SeeAsync("Getting information for database postgres");
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                    break;
                case "basebackup":
                    await SeeAsync("Creating base backup of the remote node");
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                    break;
                case "catchup" or "standby":
                    await SeeAsync("Waiting for PostgreSQL to become primary");
                    break;
                default:
                    await SeeAsync("Creating worf extension for database db2");
                    break;
            }

            Stopwatch ending = Stopwatch.StartNew();
            (int signalled, _, string error) = await ProcessRunner.RunAsync("kill", ["-TERM", tool.Id.ToString(CultureInfo.InvariantCulture)], token);
            Assert.AreEqual(0, signalled, error);
            using CancellationTokenSource exiting = CancellationTokenSource.CreateLinkedTokenSource(token);
            exiting.CancelAfter(TimeSpan.FromSeconds(120));
            await tool.WaitForExitAsync(exiting.Token);
            ending.Stop();
            string stderr = await errors;
            TestContext.WriteLine(seen.ToString());
            TestContext.WriteLine(stderr);
            Assert.AreEqual(when == "databases" ? 0 : 1, tool.ExitCode, $"the stopped tool's exit code: {stderr}");
            switch (when)
            {
                case "connect":
                    Assert.IsLessThan(TimeSpan.FromSeconds(15), ending.Elapsed, "the stop, as the tool connects to the provider that doesn't answer");
                    Assert.IsFalse(Directory.Exists(backupDir), "the data directory, which the run hadn't made");
                    break;
                case "basebackup":
                    Assert.IsFalse(Directory.Exists(backupDir), "the data directory pg_basebackup was making");
                    break;
                case "catchup" or "standby":
                    Assert.AreEqual($"False|{when == "standby"}|{before}",
                        $"{File.Exists(Path.Join(backupDir, "postmaster.pid"))}|{File.Exists(Path.Join(backupDir, "standby.signal"))}|" +
                        await File.ReadAllTextAsync(Path.Join(backupDir, "postgresql.auto.conf"), token),
                        "the server the run started, stopped, the data directory's standby.signal, and its settings, as they were");
                    break;
                default:
                    Assert.AreEqual("1|1", $"{await SubscriberAsync(provider, pgport, "SELECT count(*) FROM worf.subscription", token)}|" +
                        await SubscriberDatabaseAsync(provider, pgport, "db2", "SELECT count(*) FROM worf.subscription", token),
                        "the subscriptions of both databases, though the run was stopped after the first committed");
                    break;
            }
        }
        finally
        {
            if (paused)
            {
                _ = await ProcessRunner.RunAsync("kill", ["-CONT", postmaster], CancellationToken.None);
            }
        }

        try
        {
            if (when == "standby")
            {
                // Started without its standby.signal, the data directory comes up out of recovery, on the provider's
                // timeline, and writes WAL the provider doesn't have, so a run on it is refused at once.
                File.Delete(Path.Join(backupDir, "standby.signal"));
                (int started, string startOutput) = await provider.RunProgramAsync("pg_ctl",
                    ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", Path.Join(work.FullName, "subscriber.log")], token);
                Assert.AreEqual(0, started, startOutput);
                Assert.AreEqual("f", await SubscriberAsync(provider, pgport, "CREATE TABLE diverged (); SELECT pg_catalog.pg_is_in_recovery()", token));
                (int stopped, string stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", backupDir], token);
                Assert.AreEqual(0, stopped, stopOutput);
                Stopwatch refusing = Stopwatch.StartNew();
                (int again, _, string againErr) = await CreateSubscriberCommand.RunAsync(command, arguments, work.FullName,
                    new Dictionary<string, string> { ["PGPORT"] = port }, token);
                Assert.AreEqual($"1|True|True", $"{again}|{againErr.Contains("has run as a server of its own", StringComparison.Ordinal)}|" +
                    $"{refusing.Elapsed < TimeSpan.FromSeconds(60)}", $"a run on the data directory that ran out of recovery: {againErr}");
            }
        }
        finally
        {
            await StopAsync(provider, backupDir);
        }
    }

    // Runs SQL on one of the subscriber's databases and returns its unaligned, tuples-only output.
    private static async Task<string> SubscriberDatabaseAsync(TestNode provider, int port, string database, string sql, CancellationToken cancellationToken)
    {
        (int exitCode, string output) = await provider.RunProgramAsync("psql",
            ["-X", "-A", "-t", "-q", "-p", port.ToString(CultureInfo.InvariantCulture), "-d", database, "-v", "ON_ERROR_STOP=1", "-c", sql],
            cancellationToken);
        Assert.AreEqual(0, exitCode, output);
        return output.Trim();
    }

    // The processes a process started whose command line holds a text, by their IDs, as ps lists them.
    private static async Task<List<string>> ChildrenAsync(string parent, string command, CancellationToken token)
    {
        (int exitCode, string output, string error) = await ProcessRunner.RunAsync("ps", ["-A", "-o", "pid=,ppid=,command="], token);
        Assert.AreEqual(0, exitCode, error);
        return [.. output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.Trim().Split(' ', 3, StringSplitOptions.RemoveEmptyEntries))
            .Where(fields => fields.Length == 3 && fields[1] == parent && fields[2].Contains(command, StringComparison.Ordinal))
            .Select(fields => fields[0])];
    }

    /// <summary>
    /// Runs psql against the subscriber and checks that it succeeds, as <c>command_ok</c> does.
    /// </summary>
    private async Task AssertPsqlOkAsync(TestNode provider, int port, IReadOnlyList<string> arguments, string name,
        CancellationToken cancellationToken)
    {
        (int exitCode, string output) = await provider.RunProgramAsync("psql",
            ["-X", "-p", port.ToString(CultureInfo.InvariantCulture), "-d", Database, "-v", "ON_ERROR_STOP=1", .. arguments],
            cancellationToken);
        TestContext.WriteLine(output);
        Assert.AreEqual(0, exitCode, $"{name}: {output}");
    }

    /// <summary>
    /// Runs a query on the subscriber until it returns <c>t</c>.
    /// </summary>
    private static async Task PollSubscriberAsync(TestNode provider, int port, string sql, string what, CancellationToken cancellationToken)
    {
        Stopwatch waited = Stopwatch.StartNew();
        while (await SubscriberAsync(provider, port, sql, cancellationToken) != "t")
        {
            Assert.IsLessThan(TestNode.PollTimeout, waited.Elapsed, what);
            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }
    }

    /// <summary>
    /// Runs SQL on the subscriber and returns its unaligned, tuples-only output, or, when the subscriber may not be up,
    /// nothing for a failure.
    /// </summary>
    private static async Task<string> SubscriberAsync(TestNode provider, int port, string sql, CancellationToken cancellationToken, bool mayFail = false)
    {
        (int exitCode, string output) = await provider.RunProgramAsync("psql",
            ["-X", "-A", "-t", "-q", "-p", port.ToString(CultureInfo.InvariantCulture), "-d", Database, "-v", "ON_ERROR_STOP=1", "-c", sql],
            cancellationToken);
        if (mayFail && exitCode != 0)
        {
            return "";
        }

        Assert.AreEqual(0, exitCode, output);
        return output.Trim();
    }

    private static async Task StopAsync(TestNode provider, string dataDirectory)
    {
        if (File.Exists(Path.Join(dataDirectory, "postmaster.pid")))
        {
            _ = await provider.RunProgramAsync("pg_ctl", ["stop", "-D", dataDirectory, "-m", "immediate"], CancellationToken.None);
        }
    }
}
