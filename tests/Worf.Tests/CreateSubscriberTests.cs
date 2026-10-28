using System.Globalization;

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
    /// its new changes.
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
        ], token);
        int providerPort = provider.Cluster.Port;
        int pgport = CreateSubscriberCommand.FreePort();
        string providerDsn = $"postgresql://super@localhost:{providerPort.ToString(CultureInfo.InvariantCulture)}/postgres";
        string subscriberDsn = $"postgresql://super@localhost:{pgport.ToString(CultureInfo.InvariantCulture)}/postgres";
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
        }
        finally
        {
            await StopAsync(provider, backupDir);
        }
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

    private static async Task StopAsync(TestNode provider, string dataDirectory)
    {
        if (File.Exists(Path.Join(dataDirectory, "postmaster.pid")))
        {
            _ = await provider.RunProgramAsync("pg_ctl", ["stop", "-D", dataDirectory, "-m", "immediate"], CancellationToken.None);
        }
    }
}
