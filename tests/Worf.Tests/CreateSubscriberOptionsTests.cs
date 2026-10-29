using System.Globalization;

namespace Worf.Tests;

/// <summary>
/// Tests <c>worf_create_subscriber</c>'s options.
/// </summary>
[TestClass]
public sealed class CreateSubscriberOptionsTests
{
    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Invalid and missing options fail before the command does any work, with pglogical's messages.
    /// </summary>
    /// <param name="message">The expected start of the error output.</param>
    /// <param name="arguments">The arguments.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("No data directory specified", new[] { "-n", "sub", "--provider-dsn=dbname=postgres", "--subscriber-dsn=dbname=postgres" })]
    [DataRow("No subscriber name specified", new[] { "-D", "{data}", "--provider-dsn=dbname=postgres", "--subscriber-dsn=dbname=postgres" })]
    [DataRow("Provider connection string must be specified.", new[] { "-D", "{data}", "-n", "sub", "--subscriber-dsn=dbname=postgres" })]
    [DataRow("Subscriber connection string must be specified.", new[] { "-D", "{data}", "-n", "sub", "--provider-dsn=dbname=postgres" })]
    [DataRow("Apply delay cannot be negative.", new[] { "-D", "{data}", "-n", "sub", "--provider-dsn=dbname=postgres", "--subscriber-dsn=dbname=postgres", "--apply-delay=-1" })]
    [DataRow("Apply delay cannot be more than 86400.", new[] { "-D", "{data}", "-n", "sub", "--provider-dsn=dbname=postgres", "--subscriber-dsn=dbname=postgres", "--apply-delay=86401" })]
    [DataRow("Replication set name \"Bad!\" contains invalid character", new[] { "-D", "{data}", "-n", "sub", "--replication-sets=default,Bad!" })]
    [DataRow("The specified postgresql.conf file does not exist.", new[] { "-D", "{data}", "-n", "sub", "--postgresql-conf={missing}" })]
    [DataRow("The specified pg_hba.conf file does not exist.", new[] { "-D", "{data}", "-n", "sub", "--hba-conf={missing}" })]
    [DataRow("The specified recovery configuration file does not exist.", new[] { "-D", "{data}", "-n", "sub", "--recovery-conf={missing}" })]
    [DataRow("Either provider connection string must contain database name or --databases option must be specified.", new[] { "-D", "{data}", "-n", "sub", "--provider-dsn=host=localhost", "--subscriber-dsn=dbname=postgres" })]
    [DataRow("Invalid connection string:", new[] { "-D", "{data}", "-n", "sub", "--provider-dsn=dbname='postgres", "--subscriber-dsn=dbname=postgres" })]
    public async Task InvalidOptionsFail(string message, string[] arguments)
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-options-{Guid.NewGuid():N}");
        string command = await CreateSubscriberCommand.InstallAsync(await TestInstallations.CurrentAsync(token), Path.Join(work.FullName, "bin"));
        string[] expanded = [.. arguments.Select(argument => argument
            .Replace("{data}", Path.Join(work.FullName, "data"), StringComparison.Ordinal)
            .Replace("{missing}", Path.Join(work.FullName, "missing.conf"), StringComparison.Ordinal))];

        (int exitCode, string stdout, string stderr) = await CreateSubscriberCommand.RunAsync(command, expanded, work.FullName, null, token);

        Assert.AreEqual(1, exitCode, stdout + stderr);
        Assert.StartsWith(message, stderr, stdout + stderr);
        Assert.IsFalse(Directory.Exists(Path.Join(work.FullName, "data")), "the command created no data directory");
    }

    /// <summary>
    /// The command subscribes every listed database with the replication sets, apply delay and text transfer it's
    /// given, passes extra arguments to pg_basebackup, and stops the server when asked. It connects with a timeout
    /// longer than Npgsql's limit, which libpq takes.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SubscribesDatabasesWithOptions()
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-options-{Guid.NewGuid():N}");
        string[] databases = ["postgres", "otherdb"];

        await using TestNode provider = await TestNode.StartAsync("provider",
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "worf.synchronous_commit = true",
        ], token);
        int providerPort = provider.Cluster.Port;
        int pgport = CreateSubscriberCommand.FreePort();
        string backupDir = Path.Join(work.FullName, "backupdir");
        string command = await CreateSubscriberCommand.InstallAsync(provider.Installation, Path.Join(work.FullName, "bin"));

        _ = await provider.SafePsqlAsync("postgres", "CREATE USER super SUPERUSER", token);
        _ = await provider.SafePsqlAsync("postgres", "CREATE DATABASE otherdb", token);
        foreach (string db in databases)
        {
            _ = await provider.SafePsqlAsync(db, "CREATE EXTENSION IF NOT EXISTS worf", token);
            _ = await provider.SafePsqlAsync(db,
                $"SELECT * FROM worf.create_node(node_name := 'provider_{db}', dsn := 'dbname={db} user=super')", token);
            _ = await provider.SafePsqlAsync(db, "CREATE TABLE replicated(id integer primary key)", token);
            _ = await provider.SafePsqlAsync(db, "SELECT * FROM worf.replication_set_add_table('default', 'replicated')", token);
        }

        // As in CreateSubscriberTests, the base backup takes its port from PGPORT.
        string autoConf = Path.Join(provider.Cluster.DataDirectory, "postgresql.auto.conf");
        await File.WriteAllLinesAsync(autoConf,
            (await File.ReadAllLinesAsync(autoConf, token)).Where(static line => !line.TrimStart().StartsWith("port", StringComparison.Ordinal)),
            token);

        try
        {
            (int exitCode, string stdout, string stderr) = await CreateSubscriberCommand.RunAsync(command,
            [
                "--pgdata", backupDir, "--subscriber-name", "test_subscriber",
                $"--subscriber-dsn=host=localhost port={pgport.ToString(CultureInfo.InvariantCulture)} user=super",
                $"--provider-dsn=host=localhost port={providerPort.ToString(CultureInfo.InvariantCulture)} user=super connect_timeout=2000",
                "--databases=postgres,otherdb", "--replication-sets=default ddl_sql", "--apply-delay=5", "--text-types",
                "--extra-basebackup-args=-c fast", "--stop", "-v", "-v",
                $"--hba-conf={Path.Join(AppContext.BaseDirectory, "TestFiles", "regress-pg_hba.conf")}",
            ], work.FullName, new Dictionary<string, string> { ["PGPORT"] = pgport.ToString(CultureInfo.InvariantCulture) }, token);
            TestContext.WriteLine(stdout);
            TestContext.WriteLine(stderr);
            Assert.AreEqual(0, exitCode, "worf_create_subscriber");
            Assert.Contains("Running pg_basebackup:", stdout, "-v -v shows the commands it runs");
            Assert.Contains("-c fast", stdout, "the extra arguments go to pg_basebackup");
            Assert.IsFalse(File.Exists(Path.Join(backupDir, "postmaster.pid")), "--stop stops the server");

            (int startExit, string startOutput) = await provider.RunProgramAsync("pg_ctl",
                ["start", "-D", backupDir, "-l", Path.Join(work.FullName, "subscriber.log"), "-w", "-o", $"-p {pgport.ToString(CultureInfo.InvariantCulture)}"], token);
            Assert.AreEqual(0, startExit, startOutput);

            foreach (string db in databases)
            {
                Assert.AreEqual("test_subscriber|{default,ddl_sql}|00:00:05|t", await PsqlAsync(provider, pgport, db, """
                    SELECT s.sub_name, s.sub_replication_sets, s.sub_apply_delay, s.sub_force_text_transfer
                    FROM worf.subscription s
                    """, token), $"subscription in {db}");
                Assert.AreEqual("0", await PsqlAsync(provider, pgport, db,
                    "SELECT count(*) FROM worf.local_sync_status WHERE sync_status <> 'r'", token), $"sync status in {db}");

                _ = await provider.SafePsqlAsync(db, "INSERT INTO replicated VALUES (1)", token);
            }

            foreach (string db in databases)
            {
                string? replicated = null;
                for (int attempt = 0; attempt < 600 && replicated != "1"; attempt++)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                    replicated = await PsqlAsync(provider, pgport, db, "SELECT count(*) FROM replicated", token);
                }

                Assert.AreEqual("1", replicated, $"replication after the apply delay in {db}");
            }
        }
        finally
        {
            if (File.Exists(Path.Join(backupDir, "postmaster.pid")))
            {
                _ = await provider.RunProgramAsync("pg_ctl", ["stop", "-D", backupDir, "-m", "immediate"], CancellationToken.None);
            }

            // The subscriber's log goes with the temporary directory, so a failure shows it.
            string log = Path.Join(work.FullName, "subscriber.log");
            if (TestContext.CurrentTestOutcome != UnitTestOutcome.Passed && File.Exists(log))
            {
                TestContext.WriteLine(await File.ReadAllTextAsync(log, CancellationToken.None));
            }
        }
    }

    private static async Task<string> PsqlAsync(TestNode provider, int port, string db, string sql, CancellationToken cancellationToken)
    {
        (int exitCode, string output) = await provider.RunProgramAsync("psql",
            ["-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-p", port.ToString(CultureInfo.InvariantCulture), "-d", db, "-c", sql],
            cancellationToken);
        Assert.AreEqual(0, exitCode, output);
        return output.Trim();
    }
}
