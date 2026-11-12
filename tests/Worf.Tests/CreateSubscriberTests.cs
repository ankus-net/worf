using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Runtime.Versioning;
using System.Text;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.Internal;
using NpgsqlTypes;

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
            // The run's output is read to its end, as a shell pipe reads it, which comes as the run exits, though the server
            // it subscribed runs on.
            ProcessStartInfo start = new(command) { WorkingDirectory = work.FullName, RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (string argument in new[]
            {
                "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn={subscriberDsn}",
                $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
                $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
                $"--postgresql-conf={Path.Join(provider.Cluster.DataDirectory, "postgresql.conf")}",
            })
            {
                start.ArgumentList.Add(argument);
            }

            start.Environment["PGPORT"] = pgport.ToString(CultureInfo.InvariantCulture);
            using Process tool = Process.Start(start)!;
            Task<string> output = tool.StandardOutput.ReadToEndAsync(token);
            Task<string> errors = tool.StandardError.ReadToEndAsync(token);
            await tool.WaitForExitAsync(token);
            Task ended = Task.WhenAll(output, errors);
            bool read = await Task.WhenAny(ended, Task.Delay(TimeSpan.FromSeconds(30), token)) == ended;
            string stdout = read ? await output : "";
            string stderr = read ? await errors : "";
            TestContext.WriteLine(stdout);
            TestContext.WriteLine(stderr);
            Assert.AreEqual("0|True", $"{tool.ExitCode}|{read}", "worf_create_subscriber check, and its output, read to its end within 30 s of its exit");

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

        // The provider is reached through a relay that holds every connection made after the catch-up server's
        // walreceiver disconnects, as the server promotes at the restore point, so that the next, create_subscription's,
        // waits.
        await using ProviderRelay relay = new(provider.Cluster.Port, holdWal: false, holdAfterWalreceiver: true);
        string providerDsn = $"postgresql://super@127.0.0.1:{relay.Port.ToString(CultureInfo.InvariantCulture)}/postgres?sslmode=disable&gssencmode=disable";
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
        try
        {
            using Process tool = Process.Start(start)!;
            Task<string> errors = tool.StandardError.ReadToEndAsync(token);
            Task<string> output = tool.StandardOutput.ReadToEndAsync(token);

            // The tool is stopped once its backend waits to connect to the provider in create_subscription, in the
            // transaction that made the origin, as libpq's connection wait, or worf's own, which PostgreSQL 15 shows as an
            // extension's, shows. The server the tool starts keeps its standard output, which a killed tool leaves open.
            Task holding = relay.Held.Task;
            Assert.AreSame(holding, await Task.WhenAny(holding, output), $"the relay holding the tool's next connection to the provider: {await errors}");
            string error;
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
            relay.Pass();
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
    /// removes what the backup wrote, in the WAL directory --waldir names and the empty tablespace directory -T maps
    /// too; stopped as the server it started catches up to the restore point, through a recovery template, with no
    /// pg_controldata beside it, or streaming from the provider as the run's own settings have it, it stops the server
    /// and takes its recovery settings, the template's too, back out, though postgresql.auto.conf ended in a comment
    /// without a newline, as the template did, after each of which the next starts a line of its own, puts back the postgresql.conf and pg_hba.conf it copied over and postgresql.auto.conf, byte for
    /// byte, and keeps a standby.signal the data directory had; stopped just as the server it started leaves recovery,
    /// before the run sees it, it keeps them, as the server's cluster state shows. A run stopped before it subscribed
    /// leaves no slot it made on the provider. A data directory that has run as a server of its own, out
    /// of recovery, is refused at once, and so is one that stopped in recovery after that, as its latest checkpoint isn't
    /// the provider's, though WAL it streamed from the provider since took its own WAL's place, and one whose latest
    /// checkpoint's WAL the provider has removed. Once a database's
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
    [DataRow("streaming")]
    [DataRow("promoted")]
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

        // The streaming run's server streams from the provider through a relay that holds the WAL once its replication
        // starts, so that it never reaches the restore point.
        await using ProviderRelay relay = new(provider.Cluster.Port, holdWal: true, holdAfterWalreceiver: false);
        string providerDsn = when == "streaming"
            ? $"postgresql://super@127.0.0.1:{relay.Port.ToString(CultureInfo.InvariantCulture)}/postgres?sslmode=disable&gssencmode=disable"
            : $"postgresql://super@127.0.0.1:{providerPort}/postgres";
        string backupDir = Path.Join(work.FullName, "backupdir");
        string walDir = Path.Join(work.FullName, "wal");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));
        if (when == "catchup")
        {
            // Only a run on an existing data directory runs pg_controldata, and the cleanup doesn't need it.
            File.Delete(Path.Join(work.FullName, "bin", "pg_controldata"));
        }

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

        // The base backup streams a tablespace into a directory that's there, empty, before the data directory.
        string tablespace = Path.Join(work.FullName, "ts_old");
        string mapped = Path.Join(work.FullName, "ts_new");
        if (when == "basebackup")
        {
            _ = Directory.CreateDirectory(tablespace);
            _ = Directory.CreateDirectory(mapped);
            _ = await provider.SafePsqlAsync(Database, $"CREATE TABLESPACE ts LOCATION '{tablespace}'", token);
            _ = await provider.SafePsqlAsync(Database, "CREATE TABLE spaced (a text) TABLESPACE ts; INSERT INTO spaced SELECT repeat('x', 1000) FROM generate_series(1, 2000); CHECKPOINT", token);
        }

        // The provider's configuration files, which pg_basebackup copies, hold a comment in Latin-1, which the run puts
        // back as it was; in the streaming and catchup rows, postgresql.auto.conf's ends it without a newline, after
        // which the recovery template and the settings the run adds start lines of their own.
        foreach (string conf in new[] { "postgresql.conf", "pg_hba.conf", "postgresql.auto.conf" })
        {
            await using FileStream appending = new(Path.Join(provider.Cluster.DataDirectory, conf), FileMode.Append);
            byte[] comment = [0x0A, (byte)'#', (byte)' ', (byte)'c', (byte)'a', (byte)'f', 0xE9, .. conf == "postgresql.auto.conf" && when is "streaming" or "catchup" ? Array.Empty<byte>() : [0x0A]];
            await appending.WriteAsync(comment, token);
        }

        // The postgresql.conf the run copies over the data directory's differs from it, as pg_basebackup copies the
        // provider's, and so does the pg_hba.conf.
        string confTemplate = Path.Join(work.FullName, "postgresql.conf");
        await File.WriteAllTextAsync(confTemplate,
            await File.ReadAllTextAsync(Path.Join(provider.Cluster.DataDirectory, "postgresql.conf"), token) + "\n# the run's template\n", token);
        List<string> arguments =
        [
            "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
            $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
            $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
            $"--postgresql-conf={confTemplate}",
        ];

        // The settings the data directory starts with: the provider's, which pg_basebackup copies, or those its own base
        // backup, made as a standby's, adds. The server that catches up streams from a port nothing listens on, through
        // the recovery template, so that it never reaches the restore point.
        string before = Convert.ToHexString(await File.ReadAllBytesAsync(autoConf, token));
        string confsBefore = await ConfBytesAsync(provider.Cluster.DataDirectory, token);
        string template = Path.Join(work.FullName, "recovery.conf");
        await File.WriteAllTextAsync(template,
            $"# recovery template\nprimary_conninfo = 'host=127.0.0.1 port={CreateSubscriberCommand.FreePort().ToString(CultureInfo.InvariantCulture)} user=super'", token);
        switch (when)
        {
            case "basebackup":
                arguments.Add($"--extra-basebackup-args=--max-rate=32k --waldir={walDir} -T {tablespace}={mapped}");
                break;
            case "catchup":
                arguments.Add($"--recovery-conf={template}");
                break;
            case "standby":
                // A slot of the test's keeps the provider's WAL until the run on a directory whose checkpoint's WAL the
                // provider removed.
                _ = await provider.SafePsqlAsync(Database, "SELECT pg_catalog.pg_create_physical_replication_slot('kept_wal', true)", token);
                (int backedUp, string backupOutput) = await provider.RunProgramAsync("pg_basebackup",
                    ["-D", backupDir, "-d", providerDsn, "-X", "stream", "-R"], token);
                Assert.AreEqual(0, backedUp, backupOutput);
                before = Convert.ToHexString(await File.ReadAllBytesAsync(Path.Join(backupDir, "postgresql.auto.conf"), token));
                confsBefore = await ConfBytesAsync(backupDir, token);
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
            await using RunningTool running = new(tool);
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
                    // The run is stopped once the backup writes the tablespace, which it streams before the main archive
                    // that holds the links to it.
                    await SeeAsync("Creating base backup of the remote node");
                    Stopwatch backing = Stopwatch.StartNew();
                    while (!Directory.EnumerateFileSystemEntries(mapped).Any())
                    {
                        Assert.IsLessThan(TestNode.PollTimeout, backing.Elapsed, "the base backup's writes to the tablespace directory");
                        await Task.Delay(TimeSpan.FromMilliseconds(10), token);
                    }

                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                    break;
                case "catchup" or "standby":
                    await SeeAsync("Waiting for PostgreSQL to become primary");
                    if (when == "catchup")
                    {
                        // The template, which ends without a newline, starts a line of its own after the comment that
                        // ended without one, and so do the settings the run adds after it.
                        string recovering = await File.ReadAllTextAsync(Path.Join(backupDir, "postgresql.auto.conf"), token);
                        Assert.IsTrue(recovering.Contains("\n# recovery template\n", StringComparison.Ordinal) && recovering.Contains("user=super'\n", StringComparison.Ordinal),
                            $"the recovery template and the run's settings, each on lines of their own: {recovering}");
                    }

                    break;
                case "promoted":
                    // The run is stopped as soon as the server it started logs its promotion, before the run, which looks
                    // once a second, sees it.
                    string serverLog = Path.Join(work.FullName, "worf_create_subscriber_postgres.log");
                    Stopwatch promoting = Stopwatch.StartNew();
                    while (!File.Exists(serverLog) || !(await File.ReadAllTextAsync(serverLog, token)).Contains("selected new timeline ID", StringComparison.Ordinal))
                    {
                        Assert.IsLessThan(TestNode.PollTimeout, promoting.Elapsed, $"the server's promotion: {seen}");
                        await Task.Delay(TimeSpan.FromMilliseconds(10), token);
                    }

                    break;
                case "streaming":
                    await relay.Streaming.Task.WaitAsync(TestNode.PollTimeout, token);
                    Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
                        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_replication WHERE application_name <> 'pg_basebackup')", token),
                        "the server the run started, streaming from the provider");
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
            bool exited = true;
            try
            {
                await tool.WaitForExitAsync(exiting.Token);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                exited = false;
                tool.Kill(entireProcessTree: true);
                await tool.WaitForExitAsync(token);
            }

            ending.Stop();
            string stderr = await errors;
            TestContext.WriteLine(seen.ToString());
            TestContext.WriteLine(stderr);
            Assert.IsTrue(exited, $"the stopped tool, which hadn't ended 120 s after the stop: {stderr}");
            Assert.AreEqual(when == "databases" ? 0 : 1, tool.ExitCode, $"the stopped tool's exit code: {stderr}");
            switch (when)
            {
                case "connect":
                    Assert.IsLessThan(TimeSpan.FromSeconds(15), ending.Elapsed, "the stop, as the tool connects to the provider that doesn't answer");
                    Assert.IsFalse(Directory.Exists(backupDir), "the data directory, which the run hadn't made");
                    break;
                case "basebackup":
                    Assert.AreEqual("False|False|True|", $"{Directory.Exists(backupDir)}|{Directory.Exists(walDir)}|{Directory.Exists(mapped)}|" +
                        string.Join(',', Directory.Exists(mapped) ? Directory.EnumerateFileSystemEntries(mapped).Select(Path.GetFileName) : []),
                        "the data directory and the WAL directory pg_basebackup was making, and the tablespace directory that was there, emptied");
                    break;
                case "catchup" or "standby" or "streaming":
                    Assert.AreEqual($"False|{when == "standby"}|{before}|{confsBefore}",
                        $"{File.Exists(Path.Join(backupDir, "postmaster.pid"))}|{File.Exists(Path.Join(backupDir, "standby.signal"))}|" +
                        Convert.ToHexString(await File.ReadAllBytesAsync(Path.Join(backupDir, "postgresql.auto.conf"), token)) + "|" + await ConfBytesAsync(backupDir, token),
                        "the server the run started, stopped, the data directory's standby.signal, and its settings and configuration files, as they were");
                    break;
                case "promoted":
                    Assert.AreEqual("False|True|True", $"{File.Exists(Path.Join(backupDir, "postmaster.pid"))}|" +
                        $"{(await File.ReadAllTextAsync(Path.Join(backupDir, "postgresql.conf"), token)).Contains("# the run's template", StringComparison.Ordinal)}|" +
                        (await File.ReadAllTextAsync(Path.Join(backupDir, "postgresql.auto.conf"), token)).Contains("recovery_target_name", StringComparison.Ordinal),
                        "the server the run started, stopped, and the settings and configuration file the run gave the server that left recovery, kept");
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

        // A run stopped before it subscribed leaves no slot it made on the provider.
        if (when != "databases")
        {
            Assert.AreEqual("0", await provider.SafePsqlAsync(Database, "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_type = 'logical'", token),
                "the provider's logical slots after the stopped run");
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
                (int again, string againErr) = await RefusedRunAsync(command, arguments, work.FullName, port, token);
                Assert.AreEqual("1|True", $"{again}|{againErr.Contains("has run as a server of its own", StringComparison.Ordinal)}",
                    $"a run on the data directory that ran out of recovery: {againErr}");

                // Started again with its standby.signal, once the provider's WAL has gone past its latest checkpoint, it
                // streams the provider's WAL over its own, and stopped in recovery, it's refused at once too, as its
                // latest checkpoint is its own, which the provider's WAL doesn't hold.
                _ = await provider.SafePsqlAsync(Database, "CREATE TABLE filler AS SELECT g FROM generate_series(1, 100000) g; SELECT pg_switch_wal(); CHECKPOINT", token);
                await File.WriteAllTextAsync(Path.Join(backupDir, "standby.signal"), "", token);
                (started, startOutput) = await provider.RunProgramAsync("pg_ctl",
                    ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", Path.Join(work.FullName, "subscriber.log")], token);
                Assert.AreEqual(0, started, startOutput);
                Assert.AreEqual("t", await SubscriberAsync(provider, pgport, "SELECT pg_catalog.pg_is_in_recovery()", token));
                string subscriberLog = Path.Join(work.FullName, "subscriber.log");
                Stopwatch streaming = Stopwatch.StartNew();
                while (!(await File.ReadAllTextAsync(subscriberLog, token)).Contains("started streaming WAL from primary", StringComparison.Ordinal))
                {
                    Assert.IsLessThan(TestNode.PollTimeout, streaming.Elapsed, $"the data directory's server streaming from the provider: {await File.ReadAllTextAsync(subscriberLog, token)}");
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                }

                await Task.Delay(TimeSpan.FromSeconds(2), token);
                (stopped, stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", backupDir], token);
                Assert.AreEqual(0, stopped, stopOutput);
                (again, againErr) = await RefusedRunAsync(command, arguments, work.FullName, port, token);
                Assert.AreEqual("1|True", $"{again}|{againErr.Contains("isn't the checkpoint record its own WAL holds there", StringComparison.Ordinal)}",
                    $"a run on the data directory that ran out of recovery and stopped in recovery since: {againErr}");

                // Once the provider has removed the WAL that holds the data directory's latest checkpoint, a run on it is
                // refused at once, as it can't tell whether its WAL left the provider's, and its recovery may need WAL the
                // provider no longer has.
                _ = await provider.SafePsqlAsync(Database, """
                    SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE NOT active;
                    CREATE TABLE filler2 AS SELECT g FROM generate_series(1, 100000) g;
                    SELECT pg_catalog.pg_switch_wal();
                    CHECKPOINT;
                    """, token);
                (again, againErr) = await RefusedRunAsync(command, arguments, work.FullName, port, token);
                Assert.AreEqual("1|True", $"{again}|{againErr.Contains("isn't the checkpoint record its own WAL holds there", StringComparison.Ordinal)}",
                    $"a run on the data directory whose latest checkpoint's WAL the provider has removed: {againErr}");
            }
        }
        finally
        {
            await StopAsync(provider, backupDir);
        }
    }

    /// <summary>
    /// A run stopped before it subscribed drops only the slot it made on the provider, as it made it: a slot of the name
    /// that another run with the same subscriber name made in its place is left, with a warning, and so is the run's own
    /// slot while a walsender streams it, or once a client has confirmed what it streamed from it, whether or not it
    /// still streams; and a run whose memory is collected again and again, as a gen0 budget of 64 KB has it, still hears
    /// the stop, and drops its slot. Processes are stopped with signals, which Windows lacks.
    /// </summary>
    /// <param name="what">What became of the run's slot.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    [DataRow("replaced")]
    [DataRow("streamed")]
    [DataRow("confirmed")]
    [DataRow("moved")]
    [DataRow("collected")]
    public async Task StoppedRunsDropOnlyTheSlotsTheyMade(string what)
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-slot-{what}-{Guid.NewGuid():N}");
        await using TestNode provider = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on"], token);
        int pgport = CreateSubscriberCommand.FreePort();
        string port = pgport.ToString(CultureInfo.InvariantCulture);
        string backupDir = Path.Join(work.FullName, "backupdir");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));
        _ = await provider.SafePsqlAsync(Database, """
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            """, token);
        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)), token);
        _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);

        // The run's server catches up from a port nothing listens on, through the recovery template, so that it never
        // reaches the restore point.
        string template = Path.Join(work.FullName, "recovery.conf");
        await File.WriteAllTextAsync(template,
            $"primary_conninfo = 'host=127.0.0.1 port={CreateSubscriberCommand.FreePort().ToString(CultureInfo.InvariantCulture)} user=super'\n", token);
        ProcessStartInfo start = new(command) { WorkingDirectory = work.FullName, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string argument in new[]
        {
            "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
            $"--provider-dsn=postgresql://super@127.0.0.1:{provider.Cluster.Port.ToString(CultureInfo.InvariantCulture)}/postgres", "--drop-slot-if-exists", "-v",
            $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}", $"--recovery-conf={template}",
        })
        {
            start.ArgumentList.Add(argument);
        }

        start.Environment["PGPORT"] = port;
        if (what == "collected")
        {
            // A gen0 budget of 64 KB, which NativeAOT reads from the environment, collects every 64 KB the run allocates.
            start.Environment["DOTNET_GCgen0size"] = "10000";
        }

        using CancellationTokenSource streaming = CancellationTokenSource.CreateLinkedTokenSource(token);
        LogicalReplicationConnection? replication = null;
        Task<bool>? streamed = null;
        try
        {
            using Process tool = Process.Start(start)!;
            await using RunningTool running = new(tool);
            Task<string> errors = tool.StandardError.ReadToEndAsync(token);
            StringBuilder seen = new();
            char[] buffer = new char[4096];
            while (!seen.ToString().Contains("Waiting for PostgreSQL to become primary", StringComparison.Ordinal))
            {
                int read = await tool.StandardOutput.ReadAsync(buffer, token);
                Assert.AreNotEqual(0, read, $"the tool's output, which ended before its server caught up: {seen}{await errors}");
                _ = seen.Append(buffer, 0, read);
            }

            string slot = await provider.SafePsqlAsync(Database, "SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_type = 'logical'", token);
            if (what == "collected")
            {
                // The run waits on, allocating as it polls, and collecting as it does.
                await Task.Delay(TimeSpan.FromSeconds(5), token);
            }
            else if (what == "replaced")
            {
                // Another run with the subscriber's name drops the slot and makes its own in its place.
                _ = await provider.SafePsqlAsync(Database, $"SELECT pg_catalog.pg_drop_replication_slot('{slot}'); SELECT pg_catalog.pg_create_logical_replication_slot('{slot}', 'worf')", token);
            }
            else
            {
                NpgsqlConnectionStringBuilder super = new() { Host = "127.0.0.1", Port = provider.Cluster.Port, Database = Database, Username = "super" };
                replication = new(super.ConnectionString);
                await replication.Open(token);
                IAsyncEnumerator<XLogDataMessage> messages = replication.StartLogicalReplication(new WorfReplicationSlot(slot), streaming.Token,
                    options: [new("min_proto_version", "1"), new("max_proto_version", "1"), new("startup_params_format", "1")]).GetAsyncEnumerator(streaming.Token);
                streamed = messages.MoveNextAsync().AsTask();
                Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name = '{slot}'", token),
                    "the walsender streaming the run's slot");
                if (what is "confirmed" or "moved")
                {
                    // The client confirms a later position, and, but in the moved row, stops streaming, leaving the slot idle.
                    string made = await provider.SafePsqlAsync(Database, $"SELECT confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name = '{slot}'", token);
                    _ = await provider.SafePsqlAsync(Database, "CREATE TABLE moved (a int); INSERT INTO moved VALUES (1)", token);
                    replication.SetReplicationStatus(NpgsqlLogSequenceNumber.Parse(await provider.SafePsqlAsync(Database, "SELECT pg_catalog.pg_current_wal_lsn()", token)));
                    await replication.SendStatusUpdate(token);
                    Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
                        $"SELECT confirmed_flush_lsn > '{made}' FROM pg_catalog.pg_replication_slots WHERE slot_name = '{slot}'", token), "the client's confirmation");
                }

                if (what == "confirmed")
                {
                    await streaming.CancelAsync();
                    _ = await Task.WhenAny(streamed);
                    await replication.DisposeAsync();
                    replication = null;
                    Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"SELECT NOT active FROM pg_catalog.pg_replication_slots WHERE slot_name = '{slot}'", token),
                        "the run's slot, idle once the client stopped");
                }
            }

            Stopwatch ending = Stopwatch.StartNew();
            (int signalled, _, string error) = await ProcessRunner.RunAsync("kill", ["-TERM", tool.Id.ToString(CultureInfo.InvariantCulture)], token);
            Assert.AreEqual(0, signalled, error);
            using CancellationTokenSource exiting = CancellationTokenSource.CreateLinkedTokenSource(token);
            exiting.CancelAfter(TimeSpan.FromSeconds(120));
            await tool.WaitForExitAsync(exiting.Token);
            string stderr = await errors;
            TestContext.WriteLine($"stopped after {ending.Elapsed.TotalSeconds:F1} s: {seen}{await tool.StandardOutput.ReadToEndAsync(token)}");
            TestContext.WriteLine(stderr);
            Assert.AreEqual(what == "collected" ? "1|0|False" : "1|1|True", $"{tool.ExitCode}|" +
                await provider.SafePsqlAsync(Database, $"SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = '{slot}'", token) + "|" +
                (stderr.Contains(what == "streamed" ? $"replication slot \"{slot}\" on the provider is in use by process" : $"replication slot \"{slot}\" on the provider isn't as this run made it",
                    StringComparison.Ordinal) && (what != "moved" || stderr.Contains(", and process ", StringComparison.Ordinal))),
                $"the stopped run's exit code, the slot, which it left, and its warning: {stderr}");
        }
        finally
        {
            // The stream ends, cancelled, before its connection closes.
            await streaming.CancelAsync();
            if (streamed is not null)
            {
                _ = await Task.WhenAny(streamed);
            }

            if (replication is not null)
            {
                await replication.DisposeAsync();
            }

            await StopAsync(provider, backupDir);
        }
    }

    // Starts a standby made from the provider, has the provider switch WAL four times with a row in each segment, and
    // stops the standby once it has replayed them, returning where the provider's WAL stood.
    private static async Task<string> CatchUpStandbyAsync(TestNode provider, string backupDir, int pgport, string standbyLog, CancellationToken token)
    {
        (int started, string startOutput) = await provider.RunProgramAsync("pg_ctl",
            ["start", "-w", "-D", backupDir, "-o", $"-p {pgport.ToString(CultureInfo.InvariantCulture)}", "-l", standbyLog], token);
        Assert.AreEqual(0, started, startOutput);
        for (int i = 0; i < 4; i++)
        {
            _ = await provider.SafePsqlAsync(Database, $"INSERT INTO kept VALUES ({1001 + i}); SELECT pg_catalog.pg_switch_wal()", token);
        }

        string caughtUp = await provider.SafePsqlAsync(Database, "SELECT pg_catalog.pg_current_wal_lsn()", token);
        Stopwatch catching = Stopwatch.StartNew();
        while (await SubscriberAsync(provider, pgport, $"SELECT pg_catalog.pg_last_wal_replay_lsn() >= '{caughtUp}'", token, mayFail: true) != "t")
        {
            Assert.IsLessThan(TestNode.PollTimeout, catching.Elapsed, "the standby catching up");
            await Task.Delay(TimeSpan.FromMilliseconds(200), token);
        }

        (int stopped, string stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", backupDir], token);
        Assert.AreEqual(0, stopped, stopOutput);
        return caughtUp;
    }

    // Has the provider switch WAL a number of times, with a row in each segment, so that each switch moves it.
    private static async Task SwitchWalAsync(TestNode provider, int switches, CancellationToken token)
    {
        for (int i = 0; i < switches; i++)
        {
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO pad VALUES (1); SELECT pg_catalog.pg_switch_wal()", token);
        }
    }

    // A data directory's postgresql.conf and pg_hba.conf, byte for byte.
    private static async Task<string> ConfBytesAsync(string dataDir, CancellationToken token)
        => Convert.ToHexString(await File.ReadAllBytesAsync(Path.Join(dataDir, "postgresql.conf"), token)) + "|" +
            Convert.ToHexString(await File.ReadAllBytesAsync(Path.Join(dataDir, "pg_hba.conf"), token));

    /// <summary>
    /// A standby made with pg_basebackup -R, caught up and stopped in recovery, is subscribed though the provider has
    /// since removed the WAL that holds its latest checkpoint, as its own WAL carries its recovery to the WAL the provider
    /// still has, past where the provider's WAL begins, as the provider's own; so is one whose WAL ends inside the
    /// provider's first record there, holding only its first pages. One whose WAL ends right where the provider's
    /// begins, as one that stopped at a segment's end does, its own next segment never written, is refused, naming what
    /// keeps the provider's WAL from a standby's position, as it can't be told from one that left the provider's WAL; and
    /// so is one that ran as a server of its own from its checkpoint, writing WAL of its own past where the provider's
    /// begins, and stopped in recovery since; one whose latest checkpoint is past the provider's WAL, though the run's role
    /// on the provider may not list the provider's WAL; and, from PostgreSQL 17 on, where an online checkpoint's redo point
    /// always comes before its record, one with a torn record between them, in WAL the provider has removed, from which
    /// recovery would replay.
    /// </summary>
    /// <param name="when">How the standby's WAL meets the provider's.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("overlap")]
    [DataRow("partial")]
    [DataRow("boundary")]
    [DataRow("diverged")]
    [DataRow("ahead")]
    [DataRow("torn")]
    public async Task StandbysWhoseOwnWalReachesTheProvidersAreSubscribed(string when)
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-standby-{when}-{Guid.NewGuid():N}");
        await using TestNode provider = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on", "wal_keep_size = '32MB'"], token);
        int pgport = CreateSubscriberCommand.FreePort();
        string port = pgport.ToString(CultureInfo.InvariantCulture);
        string providerDsn = $"postgresql://super@127.0.0.1:{provider.Cluster.Port.ToString(CultureInfo.InvariantCulture)}/postgres";
        string backupDir = Path.Join(work.FullName, "backupdir");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));
        _ = await provider.SafePsqlAsync(Database, """
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            CREATE TABLE kept AS SELECT g AS id FROM generate_series(1, 1000) g;
            CREATE TABLE pad (i int);
            CHECKPOINT;
            """, token);
        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)), token);
        (int backedUp, string backupOutput) = await provider.RunProgramAsync("pg_basebackup", ["-D", backupDir, "-d", providerDsn, "-X", "stream", "-R"], token);
        Assert.AreEqual(0, backedUp, backupOutput);

        // The standby streams segments that hold no checkpoint, catches up and stops in recovery, keeping its own WAL
        // since its latest checkpoint, the base backup's.
        string standbyLog = Path.Join(work.FullName, "standby.log");
        string caughtUp = await CatchUpStandbyAsync(provider, backupDir, pgport, standbyLog, token);
        int started;
        string startOutput;
        int stopped;
        string stopOutput;

        // The segment the provider's last switch began, where the standby's WAL ends at the boundary, and where the
        // provider's WAL begins in that row.
        string boundary = await provider.SafePsqlAsync(Database, $"SELECT pg_catalog.pg_walfile_name('{caughtUp}'::pg_catalog.pg_lsn + 1)", token);
        int switches = 0;
        switch (when)
        {
            case "partial":
                // The provider's next segment begins with a record of 300 KB, which the standby streams, and holds only
                // the first 128 KB of, as one that stopped while it streamed does, its segment's file zeroed past them.
                (started, startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", standbyLog], token);
                Assert.AreEqual(0, started, startOutput);
                string large = (await provider.SafePsqlAsync(Database,
                    "SELECT pg_catalog.pg_switch_wal(); SELECT pg_catalog.pg_logical_emit_message(false, 'p', pg_catalog.repeat('x', 300000))", token)).Split('\n')[^1];
                Stopwatch streaming = Stopwatch.StartNew();
                while (await SubscriberAsync(provider, pgport, $"SELECT pg_catalog.pg_last_wal_replay_lsn() >= '{large}'", token, mayFail: true) != "t")
                {
                    Assert.IsLessThan(TestNode.PollTimeout, streaming.Elapsed, "the standby replaying the large record");
                    await Task.Delay(TimeSpan.FromMilliseconds(200), token);
                }

                (stopped, stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", backupDir], token);
                Assert.AreEqual(0, stopped, stopOutput);
                boundary = await provider.SafePsqlAsync(Database, $"SELECT pg_catalog.pg_walfile_name('{large}')", token);
                foreach (string file in Directory.EnumerateFiles(Path.Join(backupDir, "pg_wal")).Where(file =>
                    Path.GetFileName(file) is { Length: 24 } name && string.CompareOrdinal(name, boundary) > 0))
                {
                    File.Delete(file);
                }

                await using (FileStream segment = new(Path.Join(backupDir, "pg_wal", boundary), FileMode.Open, FileAccess.Write))
                {
                    _ = segment.Seek(128 * 1024, SeekOrigin.Begin);
                    await segment.WriteAsync(new byte[(int)segment.Length - (128 * 1024)], token);
                }

                switches = 2;
                break;
            case "boundary":
                // The standby's WAL ends where the segment begins, as its own files of that segment and later never were.
                foreach (string file in Directory.EnumerateFiles(Path.Join(backupDir, "pg_wal")).Where(file =>
                    Path.GetFileName(file) is { Length: 24 } name && string.CompareOrdinal(name, boundary) >= 0))
                {
                    File.Delete(file);
                }

                switches = 2;
                break;
            case "diverged":
                // Started without its standby.signal, the standby runs as a server of its own on the provider's timeline:
                // it checkpoints, then writes WAL of its own over several segments, stops at once, and stops in recovery
                // after starting again with its standby.signal, keeping that checkpoint as its latest.
                File.Delete(Path.Join(backupDir, "standby.signal"));
                (started, startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", standbyLog], token);
                Assert.AreEqual(0, started, startOutput);
                _ = await SubscriberAsync(provider, pgport, "CHECKPOINT", token);
                for (int i = 0; i < 6; i++)
                {
                    _ = await SubscriberAsync(provider, pgport, "CREATE TABLE IF NOT EXISTS own (i int); INSERT INTO own VALUES (1); SELECT pg_catalog.pg_switch_wal()", token);
                }

                (stopped, stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-m", "immediate", "-D", backupDir], token);
                Assert.AreEqual(0, stopped, stopOutput);
                await File.WriteAllTextAsync(Path.Join(backupDir, "standby.signal"), "", token);
                (started, startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", standbyLog], token);
                Assert.AreEqual(0, started, startOutput);
                (stopped, stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", backupDir], token);
                Assert.AreEqual(0, stopped, stopOutput);
                switches = 3;
                break;
            case "ahead":
                // The run's role may log in and replicate, and read the provider's worf node, but not list its WAL.
                _ = await provider.SafePsqlAsync(Database, """
                    CREATE ROLE limited LOGIN REPLICATION;
                    GRANT USAGE ON SCHEMA worf TO limited;
                    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA worf TO limited;
                    """, token);

                // Started without its standby.signal, the standby writes WAL of its own and checkpoints past where the
                // provider's WAL ends, which then stays where it is, and stops in recovery after starting again with it.
                File.Delete(Path.Join(backupDir, "standby.signal"));
                (started, startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", standbyLog], token);
                Assert.AreEqual(0, started, startOutput);
                _ = await SubscriberAsync(provider, pgport, "CREATE TABLE own (i int); INSERT INTO own SELECT g FROM generate_series(1, 100000) g; CHECKPOINT", token);
                (stopped, stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-m", "immediate", "-D", backupDir], token);
                Assert.AreEqual(0, stopped, stopOutput);
                await File.WriteAllTextAsync(Path.Join(backupDir, "standby.signal"), "", token);
                (started, startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", standbyLog], token);
                Assert.AreEqual(0, started, startOutput);
                (stopped, stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", backupDir], token);
                Assert.AreEqual(0, stopped, stopOutput);
                break;
            case "torn":
                // The standby's record at its latest checkpoint's redo point, before the checkpoint record, is torn.
                (int tornExit, string tornControl) = await provider.RunProgramAsync("pg_controldata", [backupDir], token);
                Assert.AreEqual(0, tornExit, tornControl);
                string Field(string name) => tornControl.Split('\n').Single(line => line.StartsWith(name, StringComparison.Ordinal))[name.Length..].Trim();
                string redo = Field("Latest checkpoint's REDO location:");
                if (redo == Field("Latest checkpoint location:"))
                {
                    Assert.IsLessThan(170000, int.Parse(await provider.SafePsqlAsync(Database, "SHOW server_version_num", token), CultureInfo.InvariantCulture),
                        "the server, whose online checkpoints come after their redo points from PostgreSQL 17 on");
                    return;
                }

                string redoFile = Path.Join(backupDir, "pg_wal", await provider.SafePsqlAsync(Database, $"SELECT pg_catalog.pg_walfile_name('{redo}')", token));
                long redoOffset = long.Parse(await provider.SafePsqlAsync(Database,
                    $"SELECT ('{redo}'::pg_catalog.pg_lsn - '0/0'::pg_catalog.pg_lsn) % 16777216", token), CultureInfo.InvariantCulture);
                byte[] redoBytes = await File.ReadAllBytesAsync(redoFile, token);
                redoBytes[redoOffset + 24] ^= 0x55;
                await File.WriteAllBytesAsync(redoFile, redoBytes, token);
                break;
            default:
                break;
        }

        // The provider then removes the WAL that holds the standby's checkpoint, keeping two segments behind its own.
        await SwitchWalAsync(provider, switches, token);
        string checkpointSegment = await provider.SafePsqlAsync(Database,
            "SELECT pg_catalog.pg_walfile_name(checkpoint_lsn) FROM pg_catalog.pg_control_checkpoint()", token);
        _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
        (int controlExit, string control) = await provider.RunProgramAsync("pg_controldata", [backupDir], token);
        Assert.AreEqual(0, controlExit, control);
        string standbyCheckpoint = control.Split('\n').Single(line => line.StartsWith("Latest checkpoint location:", StringComparison.Ordinal))["Latest checkpoint location:".Length..].Trim();
        Assert.AreEqual(when == "ahead" ? "t" : "f", await provider.SafePsqlAsync(Database,
            $"SELECT {(when == "ahead" ? $"'{standbyCheckpoint}'::pg_catalog.pg_lsn > pg_catalog.pg_current_wal_flush_lsn()" :
                $"EXISTS (SELECT 1 FROM pg_catalog.pg_ls_waldir() WHERE name = pg_catalog.pg_walfile_name('{standbyCheckpoint}'))")}", token),
            when == "ahead" ? $"the standby's checkpoint, {standbyCheckpoint}, past the provider's WAL"
                : $"the provider's WAL that holds the standby's checkpoint, {standbyCheckpoint}, after the provider's own at {checkpointSegment}");
        if (when is "boundary" or "partial")
        {
            Assert.AreEqual(boundary, await provider.SafePsqlAsync(Database, "SELECT pg_catalog.min(name) FROM pg_catalog.pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}$'", token),
                when == "boundary" ? "the provider's first segment, which begins where the standby's WAL ends" : "the provider's first segment, whose first record the standby holds part of");
        }

        List<string> arguments =
        [
            "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
            $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
            $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
        ];
        try
        {
            if (when == "ahead")
            {
                List<string> limited = [.. arguments.Select(argument => argument == $"--provider-dsn={providerDsn}" ? $"--provider-dsn={providerDsn.Replace("super@", "limited@", StringComparison.Ordinal)}" : argument)];
                (int refused, string refusal) = await RefusedRunAsync(command, limited, work.FullName, port, token);
                Assert.AreEqual("1|True", $"{refused}|{refusal.Contains("is past the provider's WAL", StringComparison.Ordinal)}",
                    $"a run on the standby whose checkpoint is past the provider's WAL, as a role that may not list it: {refusal}");
                return;
            }

            if (when == "torn")
            {
                (int refused, string refusal) = await RefusedRunAsync(command, arguments, work.FullName, port, token);
                Assert.AreEqual("1|True", $"{refused}|{refusal.Contains("after whole records from its redo point", StringComparison.Ordinal)}",
                    $"a run on the standby with a torn record after its checkpoint's redo point: {refusal}");
                return;
            }

            if (when is "diverged" or "boundary")
            {
                (int refused, string refusal) = await RefusedRunAsync(command, arguments, work.FullName, port, token);
                Assert.AreEqual("1|True", $"{refused}|{refusal.Contains(when == "boundary" ? "physical replication slot" : "make a new base backup", StringComparison.Ordinal)}",
                    $"a run on the standby whose WAL {(when == "boundary" ? "ends where the provider's begins" : "left the provider's")}: {refusal}");
                return;
            }

            (int exitCode, string stdout, string stderr) = await CreateSubscriberCommand.RunAsync(command, arguments, work.FullName,
                new Dictionary<string, string> { ["PGPORT"] = port }, token);
            Assert.AreEqual(0, exitCode, $"the run on the standby: {stdout}{stderr}");
            Assert.AreEqual("1004|1", $"{await SubscriberAsync(provider, pgport, "SELECT count(*) FROM kept", token)}|" +
                await SubscriberAsync(provider, pgport, "SELECT count(*) FROM worf.subscription", token), "the subscriber's rows and subscription");
        }
        finally
        {
            await StopAsync(provider, backupDir);
        }
    }

    /// <summary>
    /// A standby whose WAL isn't the provider's, though its records follow each other, is refused wherever the two
    /// overlap: with a record past its checkpoint rewritten while the provider still has that WAL; with one rewritten past
    /// the first record of the provider's first segment, once the provider has removed the standby's checkpoint's WAL;
    /// promoted after replaying a load, writing rows of its own on timeline 2, stopped before a checkpoint and stopped in
    /// recovery since, its latest checkpoint still on timeline 1; and with its last segment's tail one that PostgreSQL
    /// used again, holding another segment's WAL, past which the provider's WAL begins.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task StandbysWhoseWalIsntTheProvidersAreRefused()
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-refused-{Guid.NewGuid():N}");
        await using TestNode provider = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on", "wal_keep_size = '32MB'"], token);
        int pgport = CreateSubscriberCommand.FreePort();
        string port = pgport.ToString(CultureInfo.InvariantCulture);
        string providerDsn = $"postgresql://super@127.0.0.1:{provider.Cluster.Port.ToString(CultureInfo.InvariantCulture)}/postgres";
        string backupDir = Path.Join(work.FullName, "backupdir");
        string promotedDir = Path.Join(work.FullName, "promoteddir");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));
        _ = await provider.SafePsqlAsync(Database, """
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            CREATE TABLE kept AS SELECT g AS id FROM generate_series(1, 1000) g;
            CREATE TABLE pad (i int);
            CREATE TABLE loaded (i int);
            CHECKPOINT;
            """, token);
        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)), token);
        (int backedUp, string backupOutput) = await provider.RunProgramAsync("pg_basebackup", ["-D", backupDir, "-d", providerDsn, "-X", "stream", "-R"], token);
        Assert.AreEqual(0, backedUp, backupOutput);

        // The standby streams four segments, each holding a row and, after it, a logical message, and stops in recovery.
        string standbyLog = Path.Join(work.FullName, "standby.log");
        (int started, string startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", standbyLog], token);
        Assert.AreEqual(0, started, startOutput);
        List<string> segments = [];
        for (int i = 0; i < 4; i++)
        {
            segments.Add((await provider.SafePsqlAsync(Database, $"""
                INSERT INTO kept VALUES ({1001 + i});
                SELECT pg_catalog.pg_logical_emit_message(false, 'p', 'payload!') FROM pg_catalog.generate_series(1, 2);
                SELECT pg_catalog.pg_walfile_name(pg_catalog.pg_logical_emit_message(false, 'p', 'payload!'));
                """, token)).Split('\n')[^1]);
            _ = await provider.SafePsqlAsync(Database, "SELECT pg_catalog.pg_switch_wal()", token);
        }

        await WaitReplayedAsync(provider, pgport, await provider.SafePsqlAsync(Database, "SELECT pg_catalog.pg_current_wal_lsn()", token), token);
        (int stopped, string stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", backupDir], token);
        Assert.AreEqual(0, stopped, stopOutput);
        List<string> arguments =
        [
            "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
            $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
            $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
        ];
        List<string> problems = [];
        async Task RefusedAsync(string what, string expected, List<string> run)
        {
            (int refused, string refusal) = await RefusedRunAsync(command, run, work.FullName, port, token);
            if (refused != 1 || !refusal.Contains(expected, StringComparison.Ordinal))
            {
                problems.Add($"{what}: exit {refused}, {refusal.Trim()}");
            }
        }

        try
        {
            // The provider still has the WAL from the standby's checkpoint on, where the standby's first message differs.
            string first = Path.Join(backupDir, "pg_wal", segments[0]);
            byte[] kept = await RewriteMessageAsync(provider, first, token);
            await RefusedAsync("a message rewritten where the provider still has the WAL", "isn't the provider's", arguments);
            await File.WriteAllBytesAsync(first, kept, token);

            // The provider removes the standby's checkpoint's WAL, keeping two segments behind its own, so that its first
            // segment is the standby's third, where the standby's message after that segment's first record differs.
            _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
            Assert.AreEqual(segments[2], await provider.SafePsqlAsync(Database, "SELECT pg_catalog.min(name) FROM pg_catalog.pg_ls_waldir() WHERE name ~ '^[0-9A-F]{24}$'", token),
                "the provider's first segment");
            string third = Path.Join(backupDir, "pg_wal", segments[2]);
            kept = await RewriteMessageAsync(provider, third, token);
            await RefusedAsync("a message rewritten past the first record of the provider's first segment", "isn't the provider's", arguments);
            await File.WriteAllBytesAsync(third, kept, token);

            // A copy of the standby replays a load, is promoted, writes rows of its own and stops at once, before the
            // checkpoint the promotion asked for has written the load out, and stops in recovery after starting again.
            CopyDirectory(backupDir, promotedDir);
            (started, startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", promotedDir, "-o", $"-p {port}", "-l", standbyLog], token);
            Assert.AreEqual(0, started, startOutput);
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO loaded SELECT g FROM generate_series(1, 300000) g", token);
            try
            {
                await WaitReplayedAsync(provider, pgport, await provider.SafePsqlAsync(Database, "SELECT pg_catalog.pg_current_wal_lsn()", token), token);
            }
            catch (AssertFailedException)
            {
                TestContext.WriteLine(await File.ReadAllTextAsync(standbyLog, token));
                throw;
            }

            _ = await SubscriberAsync(provider, pgport, "SELECT pg_catalog.pg_promote()", token);
            _ = await SubscriberAsync(provider, pgport, "CREATE TABLE own (i int); INSERT INTO own SELECT g FROM generate_series(1, 25) g", token);
            (stopped, stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-m", "immediate", "-D", promotedDir], token);
            Assert.AreEqual(0, stopped, stopOutput);
            await File.WriteAllTextAsync(Path.Join(promotedDir, "standby.signal"), "", token);
            (started, startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", promotedDir, "-o", $"-p {port}", "-l", standbyLog], token);
            Assert.AreEqual(0, started, startOutput);
            (stopped, stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", promotedDir], token);
            Assert.AreEqual(0, stopped, stopOutput);
            (int controlExit, string control) = await provider.RunProgramAsync("pg_controldata", [promotedDir], token);
            Assert.AreEqual(0, controlExit, control);
            Assert.AreEqual("1", control.Split('\n').Single(line => line.StartsWith("Latest checkpoint's TimeLineID:", StringComparison.Ordinal))
                ["Latest checkpoint's TimeLineID:".Length..].Trim(), "the promoted copy's latest checkpoint, still on timeline 1");
            _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
            await RefusedAsync("a copy promoted to timeline 2", "timeline 2", [.. arguments.Select(argument => argument == backupDir ? promotedDir : argument)]);

            // The provider's WAL then begins past the standby's, whose last segment's tail holds its first segment's WAL,
            // as a file PostgreSQL used again does.
            await SwitchWalAsync(provider, 3, token);
            _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
            foreach (string file in Directory.EnumerateFiles(Path.Join(backupDir, "pg_wal")).Where(file =>
                Path.GetFileName(file) is { Length: 24 } name && string.CompareOrdinal(name, segments[3]) > 0))
            {
                File.Delete(file);
            }

            string last = Path.Join(backupDir, "pg_wal", segments[3]);
            byte[] lastBytes = await File.ReadAllBytesAsync(last, token);
            (await File.ReadAllBytesAsync(first, token)).AsSpan(16384).CopyTo(lastBytes.AsSpan(16384));
            await File.WriteAllBytesAsync(last, lastBytes, token);
            await RefusedAsync("a standby whose last segment's tail was used again", "physical replication slot", arguments);
            Assert.AreEqual("", string.Join("\n", problems));
        }
        finally
        {
            await StopAsync(provider, backupDir);
            await StopAsync(provider, promotedDir);
        }
    }

    /// <summary>
    /// A standby of an old primary is subscribed to its sibling that was promoted: though the standby's WAL holds the
    /// start of a record the old primary wrote past where the sibling left the old timeline, which recovery that follows
    /// the sibling never replays. One that replayed past where the sibling, promoted at a restore point, left the old
    /// timeline, and has fetched the sibling's history of the new timeline it can't follow, is refused at once, rather than
    /// recovering to a server that can't start.
    /// </summary>
    /// <param name="when">How the standby's WAL meets where the sibling left the old timeline.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("sibling")]
    [DataRow("history")]
    public async Task StandbysFollowAPromotedSiblingOnlyFromWhereItLeft(string when)
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-sibling-{when}-{Guid.NewGuid():N}");
        await using TestNode primary = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on", "wal_keep_size = '64MB'"], token);
        int siblingPort = CreateSubscriberCommand.FreePort();
        int standbyPort = CreateSubscriberCommand.FreePort();
        string primaryDsn = $"postgresql://super@127.0.0.1:{primary.Cluster.Port.ToString(CultureInfo.InvariantCulture)}/postgres";
        string siblingDir = Path.Join(work.FullName, "sibling");
        string standbyDir = Path.Join(work.FullName, "standby");
        string siblingLog = Path.Join(work.FullName, "sibling.log");
        string standbyLog = Path.Join(work.FullName, "standby.log");
        string command = await CreateSubscriberCommand.InstallAsync(primary.Installation, Path.Join(work.FullName, "bin"));
        _ = await primary.SafePsqlAsync(Database, """
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            CREATE TABLE kept AS SELECT g AS id FROM generate_series(1, 1000) g;
            CHECKPOINT;
            """, token);
        string autoConf = Path.Join(primary.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)), token);
        foreach (string dir in new[] { siblingDir, standbyDir })
        {
            (int backedUp, string backupOutput) = await primary.RunProgramAsync("pg_basebackup", ["-D", dir, "-d", primaryDsn, "-X", "stream", "-R"], token);
            Assert.AreEqual(0, backedUp, backupOutput);
        }

        async Task StartAsync(string dir, int port, string log)
        {
            (int started, string startOutput) = await primary.RunProgramAsync("pg_ctl",
                ["start", "-w", "-D", dir, "-o", $"-p {port.ToString(CultureInfo.InvariantCulture)}", "-l", log], token);
            Assert.AreEqual(0, started, startOutput);
        }

        async Task StopServerAsync(string dir)
        {
            (int stopped, string stopOutput) = await primary.RunProgramAsync("pg_ctl", ["stop", "-w", "-D", dir], token);
            Assert.AreEqual(0, stopped, stopOutput);
        }

        try
        {
            string expected;
            if (when == "sibling")
            {
                // Both standbys replay a row and stop; the old primary then writes a record of 300 KB and stops, and the
                // standby's WAL is given the old primary's up to the middle of it, as one that streamed part of it has.
                await StartAsync(siblingDir, siblingPort, siblingLog);
                await StartAsync(standbyDir, standbyPort, standbyLog);
                string inserted = (await primary.SafePsqlAsync(Database, "INSERT INTO kept VALUES (1001); SELECT pg_catalog.pg_current_wal_lsn()", token)).Split('\n')[^1];
                await WaitReplayedAsync(primary, siblingPort, inserted, token);
                await WaitReplayedAsync(primary, standbyPort, inserted, token);
                await StopServerAsync(siblingDir);
                await StopServerAsync(standbyDir);
                string end = await primary.SafePsqlAsync(Database, "SELECT pg_catalog.pg_logical_emit_message(false, 'p', pg_catalog.repeat('x', 300000))", token);
                string segment = await primary.SafePsqlAsync(Database, $"SELECT pg_catalog.pg_walfile_name('{end}'::pg_catalog.pg_lsn - 150000)", token);
                long middle = long.Parse(await primary.SafePsqlAsync(Database,
                    $"SELECT (('{end}'::pg_catalog.pg_lsn - 150000) - '0/0'::pg_catalog.pg_lsn) % 16777216", token), CultureInfo.InvariantCulture);
                await primary.StopAsync("fast", token);
                byte[] theirs = await File.ReadAllBytesAsync(Path.Join(primary.Cluster.DataDirectory, "pg_wal", segment), token);
                string ours = Path.Join(standbyDir, "pg_wal", segment);
                byte[] mine = File.Exists(ours) ? await File.ReadAllBytesAsync(ours, token) : new byte[theirs.Length];
                theirs.AsSpan(0, (int)middle).CopyTo(mine);
                await File.WriteAllBytesAsync(ours, mine, token);

                // The sibling is promoted where its WAL ends, before the large record.
                await StartAsync(siblingDir, siblingPort, siblingLog);
                (int promoted, string promoteOutput) = await primary.RunProgramAsync("pg_ctl", ["promote", "-w", "-D", siblingDir], token);
                Assert.AreEqual(0, promoted, promoteOutput);
                expected = "subscribed";
            }
            else
            {
                // The sibling applies a second late and is promoted at a restore point; the standby replays a row past it,
                // and the old primary stops.
                await File.AppendAllTextAsync(Path.Join(siblingDir, "postgresql.auto.conf"),
                    "recovery_target_name = 'fork'\nrecovery_target_action = 'promote'\nrecovery_min_apply_delay = '8s'\n", token);
                await StartAsync(siblingDir, siblingPort, siblingLog);
                await StartAsync(standbyDir, standbyPort, standbyLog);
                string past = (await primary.SafePsqlAsync(Database, """
                    INSERT INTO kept VALUES (1001); SELECT pg_catalog.pg_create_restore_point('fork'); INSERT INTO kept VALUES (1002);
                    SELECT pg_catalog.pg_current_wal_lsn();
                    """, token)).Split('\n')[^1];
                await WaitReplayedAsync(primary, standbyPort, past, token);
                await StopServerAsync(standbyDir);
                Stopwatch promoting = Stopwatch.StartNew();
                while (await SubscriberAsync(primary, siblingPort, "SELECT pg_catalog.pg_is_in_recovery()", token, mayFail: true) != "f")
                {
                    Assert.IsLessThan(TestNode.PollTimeout, promoting.Elapsed, "the sibling promoted at the restore point");
                    await Task.Delay(TimeSpan.FromMilliseconds(200), token);
                }

                await primary.StopAsync("fast", token);

                // The standby, pointed at the sibling, fetches its history of timeline 2, which it can't follow, and stops.
                await File.AppendAllTextAsync(Path.Join(standbyDir, "postgresql.auto.conf"),
                    $"primary_conninfo = 'host=127.0.0.1 port={siblingPort.ToString(CultureInfo.InvariantCulture)} user=super'\n", token);
                await StartAsync(standbyDir, standbyPort, standbyLog);
                Stopwatch fetching = Stopwatch.StartNew();
                while (!File.Exists(Path.Join(standbyDir, "pg_wal", "00000002.history")))
                {
                    Assert.IsLessThan(TestNode.PollTimeout, fetching.Elapsed, $"the standby fetching the sibling's history: {await File.ReadAllTextAsync(standbyLog, token)}");
                    await Task.Delay(TimeSpan.FromMilliseconds(200), token);
                }

                await StopServerAsync(standbyDir);
                expected = "where the provider left that timeline";
            }

            List<string> arguments =
            [
                "-D", standbyDir, "--subscriber-name=test_subscriber",
                $"--subscriber-dsn=postgresql://super@127.0.0.1:{standbyPort.ToString(CultureInfo.InvariantCulture)}/postgres",
                $"--provider-dsn=postgresql://super@127.0.0.1:{siblingPort.ToString(CultureInfo.InvariantCulture)}/postgres", "--drop-slot-if-exists", "-v",
                $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
            ];
            if (expected == "subscribed")
            {
                (int exitCode, string stdout, string stderr) = await CreateSubscriberCommand.RunAsync(command, arguments, work.FullName,
                    new Dictionary<string, string> { ["PGPORT"] = standbyPort.ToString(CultureInfo.InvariantCulture) }, token);
                Assert.AreEqual(0, exitCode, $"the run on the standby: {stdout}{stderr}");
                Assert.AreEqual("1001|1", $"{await SubscriberAsync(primary, standbyPort, "SELECT count(*) FROM kept", token)}|" +
                    await SubscriberAsync(primary, standbyPort, "SELECT count(*) FROM worf.subscription", token), "the subscriber's rows and subscription");
            }
            else
            {
                (int refused, string refusal) = await RefusedRunAsync(command, arguments, work.FullName, standbyPort.ToString(CultureInfo.InvariantCulture), token);
                Assert.AreEqual("1|True", $"{refused}|{refusal.Contains(expected, StringComparison.Ordinal)}", $"a run on the standby past where the sibling left: {refusal}");
            }
        }
        finally
        {
            await StopAsync(primary, standbyDir);
            await StopAsync(primary, siblingDir);
        }
    }

    /// <summary>
    /// A standby whose WAL holds a record its provider's crash cut short, which the provider overwrote as it started
    /// again, as recovery passes over it, is compared past it: a message rewritten after it, on the standby's last page,
    /// is refused.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task StandbysPastAnOverwrittenRecordAreCompared()
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-overwritten-{Guid.NewGuid():N}");
        await using TestNode provider = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on", "wal_keep_size = '64MB'"], token);
        int pgport = CreateSubscriberCommand.FreePort();
        string port = pgport.ToString(CultureInfo.InvariantCulture);
        string providerDsn = $"postgresql://super@127.0.0.1:{provider.Cluster.Port.ToString(CultureInfo.InvariantCulture)}/postgres";
        string backupDir = Path.Join(work.FullName, "backupdir");
        string standbyLog = Path.Join(work.FullName, "standby.log");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));
        _ = await provider.SafePsqlAsync(Database, """
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            CREATE TABLE kept AS SELECT g AS id FROM generate_series(1, 1000) g;
            CREATE TABLE pad (i int);
            CHECKPOINT;
            """, token);
        // The backup takes no port of the provider's, which the provider takes again as it restarts.
        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        string[] settings = await File.ReadAllLinesAsync(autoConf, token);
        await File.WriteAllLinesAsync(autoConf, settings.Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)), token);
        (int backedUp, string backupOutput) = await provider.RunProgramAsync("pg_basebackup", ["-D", backupDir, "-d", providerDsn, "-X", "stream", "-R"], token);
        Assert.AreEqual(0, backedUp, backupOutput);
        await File.WriteAllLinesAsync(autoConf, settings, token);
        try
        {
            // The provider fills most of a new segment, writes a record of 2 MB that crosses into the next, flushes it
            // with a commit, stops at once and loses that next segment, so that starting again it overwrites the record's
            // missing part, and then writes messages.
            _ = await provider.SafePsqlAsync(Database, """
                SELECT pg_catalog.pg_switch_wal();
                SELECT count(pg_catalog.pg_logical_emit_message(false, 'p', pg_catalog.repeat('x', 1000000))) FROM pg_catalog.generate_series(1, 15);
                """, token);
            string end = await provider.SafePsqlAsync(Database, "SELECT pg_catalog.pg_logical_emit_message(false, 'p', pg_catalog.repeat('y', 2000000))", token);
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO pad VALUES (1)", token);
            string lost = await provider.SafePsqlAsync(Database, $"SELECT pg_catalog.pg_walfile_name('{end}')", token);
            Assert.AreNotEqual(lost, await provider.SafePsqlAsync(Database, $"SELECT pg_catalog.pg_walfile_name('{end}'::pg_catalog.pg_lsn - 2000000)", token),
                "the segments the large record's start and end are in");
            await provider.StopAsync("immediate", token);
            File.Delete(Path.Join(provider.Cluster.DataDirectory, "pg_wal", lost));
            await provider.StartServerAsync(token);
            string written = (await provider.SafePsqlAsync(Database, """
                INSERT INTO pad VALUES (2);
                SELECT pg_catalog.pg_logical_emit_message(false, 'p', 'payload!') FROM pg_catalog.generate_series(1, 3);
                SELECT pg_catalog.pg_current_wal_insert_lsn();
                """, token)).Split('\n')[^1];

            // The standby streams it all, replays past the overwritten record, and stops at once, its latest checkpoint before
            // the record; a message after the record, on the standby's last page, is rewritten.
            (int started, string startOutput) = await provider.RunProgramAsync("pg_ctl", ["start", "-w", "-D", backupDir, "-o", $"-p {port}", "-l", standbyLog], token);
            Assert.AreEqual(0, started, startOutput);
            await WaitReplayedAsync(provider, pgport, written, token);
            (int stopped, string stopOutput) = await provider.RunProgramAsync("pg_ctl", ["stop", "-w", "-m", "immediate", "-D", backupDir], token);
            Assert.AreEqual(0, stopped, stopOutput);
            (int controlExit, string control) = await provider.RunProgramAsync("pg_controldata", [backupDir], token);
            Assert.AreEqual(0, controlExit, control);
            string latest = control.Split('\n').Single(line => line.StartsWith("Latest checkpoint location:", StringComparison.Ordinal))["Latest checkpoint location:".Length..].Trim();
            Assert.AreEqual("t", await provider.SafePsqlAsync(Database, $"SELECT '{latest}'::pg_catalog.pg_lsn < '{end}'::pg_catalog.pg_lsn - 2000000", token),
                $"the standby's latest checkpoint, {latest}, before the record the provider overwrote");
            // The standby's WAL ends on the page of the last message, as one that stopped while it streamed does, its
            // segment's file zeroed past it and its later ones never written.
            (_, string dump) = await provider.RunProgramAsync("pg_waldump", ["--rmgr=LogicalMessage", "--path", Path.Join(backupDir, "pg_wal"), lost], token);
            int lastPage = dump.Split('\n').Where(line => line.Contains("lsn: ", StringComparison.Ordinal))
                .Select(line => (int)(uint.Parse(line[(line.IndexOf("lsn: ", StringComparison.Ordinal) + 5)..line.IndexOf(',', line.IndexOf("lsn: ", StringComparison.Ordinal))].Split('/')[1],
                    NumberStyles.HexNumber, CultureInfo.InvariantCulture) % 16777216) / 8192).Max();
            foreach (string file in Directory.EnumerateFiles(Path.Join(backupDir, "pg_wal")).Where(file =>
                Path.GetFileName(file) is { Length: 24 } name && string.CompareOrdinal(name, lost) > 0))
            {
                File.Delete(file);
            }

            await using (FileStream segment = new(Path.Join(backupDir, "pg_wal", lost), FileMode.Open, FileAccess.Write))
            {
                _ = segment.Seek((lastPage + 1) * 8192, SeekOrigin.Begin);
                await segment.WriteAsync(new byte[(int)segment.Length - ((lastPage + 1) * 8192)], token);
            }

            _ = await RewriteMessageAsync(provider, Path.Join(backupDir, "pg_wal", lost), token, last: true);
            List<string> arguments =
            [
                "-D", backupDir, "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
                $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v",
                $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
            ];
            (int refused, string refusal) = await RefusedRunAsync(command, arguments, work.FullName, port, token);
            Assert.AreEqual("1|True", $"{refused}|{refusal.Contains("isn't the provider's", StringComparison.Ordinal)}",
                $"a run on the standby with a message rewritten past the overwritten record: {refusal}");
        }
        finally
        {
            await StopAsync(provider, backupDir);
        }
    }

    // Waits for the standby at a port to have replayed the provider's WAL to a position.
    private static async Task WaitReplayedAsync(TestNode provider, int pgport, string position, CancellationToken token)
    {
        Stopwatch catching = Stopwatch.StartNew();
        while (await SubscriberAsync(provider, pgport, $"SELECT pg_catalog.pg_last_wal_replay_lsn() >= '{position}'", token, mayFail: true) != "t")
        {
            Assert.IsLessThan(TestNode.PollTimeout, catching.Elapsed,
                $"the standby catching up to {position}, at {await SubscriberAsync(provider, pgport, "SELECT pg_catalog.pg_last_wal_replay_lsn()", token, mayFail: true)}");
            await Task.Delay(TimeSpan.FromMilliseconds(200), token);
        }
    }

    // Rewrites the last byte of the first logical message, or the last, in a WAL segment's file that doesn't cross a
    // page, and its record's checksum, so that its records still follow each other but are no longer the provider's;
    // returns the file's bytes as they were.
    private static async Task<byte[]> RewriteMessageAsync(TestNode provider, string segment, CancellationToken token, bool last = false)
    {
        const int Page = 8192;
        byte[] original = await File.ReadAllBytesAsync(segment, token);
        // pg_waldump splits a file's path at '/' alone, so the directory goes in --path.
        (_, string dump) = await provider.RunProgramAsync("pg_waldump", ["--rmgr=LogicalMessage", "--path", Path.GetDirectoryName(segment)!, Path.GetFileName(segment)], token);
        List<(int Offset, int Total)> messages = [.. dump.Split('\n').Where(line => line.Contains("lsn: ", StringComparison.Ordinal))
            .Select(line => line[(line.IndexOf("lsn: ", StringComparison.Ordinal) + 5)..line.IndexOf(',', line.IndexOf("lsn: ", StringComparison.Ordinal))])
            .Select(lsn => (int)(uint.Parse(lsn[(lsn.IndexOf('/', StringComparison.Ordinal) + 1)..], NumberStyles.HexNumber, CultureInfo.InvariantCulture) % (uint)original.Length))
            .Select(at => (Offset: at, Total: BitConverter.ToInt32(original, at)))
            .Where(message => (message.Offset % Page) + message.Total <= Page)];
        (int offset, int total) = messages.Count == 0 ? (-1, 0) : last ? messages[^1] : messages[0];
        Assert.IsGreaterThanOrEqualTo(0, offset, $"a logical message within its page in {segment}: {dump}");
        byte[] rewritten = [.. original];
        rewritten[offset + total - 1] ^= 1;
        uint crc = Crc32C(0xFFFFFFFF, rewritten.AsSpan(offset + 24, total - 24));
        crc = Crc32C(crc, rewritten.AsSpan(offset, 20)) ^ 0xFFFFFFFF;
        BitConverter.GetBytes(crc).CopyTo(rewritten, offset + 20);
        await File.WriteAllBytesAsync(segment, rewritten, token);
        return original;

        static uint Crc32C(uint crc, ReadOnlySpan<byte> data)
        {
            foreach (byte b in data)
            {
                crc = BitOperations.Crc32C(crc, b);
            }

            return crc;
        }
    }

    // Copies a stopped server's data directory.
    private static void CopyDirectory(string from, string to)
    {
        foreach (string directory in Directory.EnumerateDirectories(from, "*", SearchOption.AllDirectories))
        {
            _ = Directory.CreateDirectory(Path.Join(to, Path.GetRelativePath(from, directory)));
        }

        _ = Directory.CreateDirectory(to);
        foreach (string file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            File.Copy(file, Path.Join(to, Path.GetRelativePath(from, file)));
        }

        // The server starts only in a data directory its owner alone may enter.
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(to, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
    }

    /// <summary>
    /// A tablespace directory -T maps that the run can't list fails the run as pg_basebackup refuses it, rather than
    /// ending the tool unhandled, and the run leaves no slot on the provider. On Linux and macOS, where the directory's
    /// mode keeps its owner from listing it.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    [SupportedOSPlatform("linux")]
    [SupportedOSPlatform("macos")]
    public async Task UnlistableTablespaceDirectoriesFailTheRun()
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-unlistable-{Guid.NewGuid():N}");
        await using TestNode provider = await TestNode.StartAsync("provider",
            ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10", "track_commit_timestamp = on"], token);
        int pgport = CreateSubscriberCommand.FreePort();
        string port = pgport.ToString(CultureInfo.InvariantCulture);
        string providerDsn = $"postgresql://super@127.0.0.1:{provider.Cluster.Port.ToString(CultureInfo.InvariantCulture)}/postgres";
        string tablespace = Path.Join(work.FullName, "ts_old");
        string mapped = Path.Join(work.FullName, "ts_new");
        _ = Directory.CreateDirectory(tablespace);
        _ = Directory.CreateDirectory(mapped);
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));
        _ = await provider.SafePsqlAsync(Database, $"""
            CREATE USER super SUPERUSER;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super');
            """, token);
        _ = await provider.SafePsqlAsync(Database, $"CREATE TABLESPACE ts LOCATION '{tablespace}'", token);
        _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
        File.SetUnixFileMode(mapped, UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        try
        {
            (int exitCode, string stdout, string stderr) = await CreateSubscriberCommand.RunAsync(command,
            [
                "-D", Path.Join(work.FullName, "backupdir"), "--subscriber-name=test_subscriber", $"--subscriber-dsn=postgresql://super@127.0.0.1:{port}/postgres",
                $"--provider-dsn={providerDsn}", "--drop-slot-if-exists", "-v", $"--extra-basebackup-args=-T {tablespace}={mapped}",
                $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
            ], work.FullName, new Dictionary<string, string> { ["PGPORT"] = port }, token);
            Assert.AreEqual("1|True", $"{exitCode}|{stderr.Contains("Permission denied", StringComparison.Ordinal)}", $"the run: {stdout}{stderr}");
            Assert.AreEqual("0", await provider.SafePsqlAsync(Database, "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_type = 'logical'", token),
                "the provider's logical slots after the failed run");
        }
        finally
        {
            File.SetUnixFileMode(mapped, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
    }

    // Runs the tool on a data directory it's to refuse at once, for 60 seconds at most, after which it's stopped, as is
    // the server it may have started, and its exit code and standard error are returned.
    private static async Task<(int ExitCode, string Stderr)> RefusedRunAsync(string command, List<string> arguments, string workingDirectory,
        string port, CancellationToken token)
    {
        using CancellationTokenSource bound = CancellationTokenSource.CreateLinkedTokenSource(token);
        bound.CancelAfter(TimeSpan.FromSeconds(60));
        ProcessStartInfo start = new(command) { WorkingDirectory = workingDirectory, RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        start.Environment["PGPORT"] = port;
        using Process tool = Process.Start(start)!;
        Task<string> output = tool.StandardOutput.ReadToEndAsync(token);
        Task<string> errors = tool.StandardError.ReadToEndAsync(token);
        bool timedOut = false;
        try
        {
            await tool.WaitForExitAsync(bound.Token);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            timedOut = true;
            _ = await ProcessRunner.RunAsync("kill", ["-TERM", tool.Id.ToString(CultureInfo.InvariantCulture)], CancellationToken.None);
            if (!tool.WaitForExit(TimeSpan.FromSeconds(30)))
            {
                tool.Kill(true);
            }
        }

        Assert.IsFalse(timedOut, $"the run, still going after 60 s rather than refused at once: {await output}{await errors}");
        _ = await output;
        return (tool.ExitCode, await errors);
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
