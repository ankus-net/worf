using Ankus.PgConfig;
using Ankus.Testing;

namespace Worf.Tests;

/// <summary>
/// Checks that structure synchronization runs pg_dump and pg_restore from an installation whose path has spaces, such as
/// one under <c>C:\Program Files</c> on Windows.
/// </summary>
[TestClass]
public sealed class InstallationPathTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// A subscription with structure synchronization creates the provider's tables on the subscriber, running the
    /// installation's pg_dump and pg_restore from under <c>Program Files</c>.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task StructureSyncFromProgramFiles()
    {
        CancellationToken token = TestContext.CancellationToken;
        PostgresInstallation current = await TestInstallations.CurrentAsync(token);
        await using PostgresTestInstallation? staged = current.BinDirectory.Contains(' ', StringComparison.Ordinal)
            ? null
            : await StageAsync(current, token);
        PostgresInstallation installation = staged?.Installation ?? current;
        TestContext.WriteLine($"PostgreSQL installed in {installation.BinDirectory}");
        Assert.Contains(' ', installation.BinDirectory, "the installation's path has a space");

        string[] configuration =
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "worf.synchronous_commit = true",
        ];
        await using TestNode provider = await TestNode.StartAsync("provider", configuration, token, installation);
        await using TestNode subscriber = await TestNode.StartAsync("subscriber", configuration, token, installation);

        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER", token);
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION worf", token);
            _ = await node.SafePsqlAsync(Database,
                $"SELECT * FROM worf.create_node(node_name := '{node.Name}', dsn := '{node.Dsn(Database, SuperUser)}')", token);
        }

        _ = await provider.SafePsqlAsync(Database, "CREATE TABLE public.structure_sync(id integer primary key, data text)", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO structure_sync VALUES (1, 'one'), (2, 'two')", token);
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'structure_sync')", token);

        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT * FROM worf.create_subscription(
                subscription_name := 'test_subscription',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := true,
                synchronize_data := true)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.show_subscription_status() WHERE status = 'replicating')", token),
            $"subscription failed to reach \"replicating\" state: {subscriber.ReadLog()}");
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.wait_for_subscription_sync_complete('test_subscription')", token);

        Assert.AreEqual("1|one\n2|two", await subscriber.SafePsqlAsync(Database,
            "SELECT id, data FROM structure_sync ORDER BY id", token), "the table and its rows reached the subscriber");
    }

    /// <summary>
    /// Copies the installation under <c>Program Files</c>: the system's folder on Windows when it's writable, as on
    /// the CI runners, and otherwise one in the temporary directory.
    /// </summary>
    private static async Task<PostgresTestInstallation> StageAsync(PostgresInstallation installation, CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
        {
            try
            {
                return await PostgresTestInstallation.StageAsync(installation,
                    Path.Join(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), $"worf-tests-{Guid.NewGuid():N}"),
                    cancellationToken);
            }
            catch (UnauthorizedAccessException)
            {
                // The account can't write to Program Files.
            }
        }

        return await PostgresTestInstallation.StageAsync(installation,
            Path.Join(Path.GetTempPath(), $"worf-tests-{Guid.NewGuid():N}", "Program Files"), cancellationToken);
    }
}
