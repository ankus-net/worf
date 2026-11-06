using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Npgsql;

namespace Worf.Tests;

/// <summary>
/// Waits on a provider that stops answering, or on a postmaster that doesn't start a worker, end, as PostgreSQL's own
/// apply worker and launcher bound them, and replication goes on once what held it does.
/// </summary>
[TestClass]
public sealed class StalledPeerTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Apply asks a provider that sends nothing for a reply once half of wal_receiver_timeout has passed, and stops
    /// once all of it has, as PostgreSQL's apply worker does. A call it makes to its provider as it starts, here a
    /// connection that a stopped postmaster never accepts, gives up after wal_receiver_timeout and names what it waited
    /// for, and so does a table sync's call to a server that takes the connection and never answers. The manager gives
    /// up on a worker that the postmaster hasn't started after wal_receiver_timeout, with a warning naming its
    /// subscription, rather than wait for it while every other worker of the database waits on the manager. Each
    /// subscription replicates again once the process goes on. Processes are stopped with signals, which Windows lacks.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public async Task StalledProvidersAndWorkerStartsTimeOut()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT FROM worf.show_subscription_status() WHERE status = 'replicating')", token), "the subscription replicating");
        await AppliesAsync(provider, subscriber, 1, token);
        string slot = await subscriber.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription", token);

        // The provider's walsender stops: apply's request for a reply goes unanswered, and it stops.
        int logpos = subscriber.ReadLog().Length;
        string walsender = await provider.SafePsqlAsync(Database, $"SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{slot}'", token);
        await SignalAsync("-STOP", walsender, token);
        try
        {
            Assert.IsTrue(await subscriber.PollLogAsync(logpos,
                "terminating worf apply worker for subscription \"sub\" due to timeout", token), "apply stopping for the silent provider");
        }
        finally
        {
            await SignalAsync("-CONT", walsender, token);
        }

        await AppliesAsync(provider, subscriber, 2, token);

        // The provider's postmaster stops: the restarted apply worker's connection, which the kernel accepts, gets no
        // answer.
        logpos = subscriber.ReadLog().Length;
        string providerPostmaster = Postmaster(provider);
        await SignalAsync("-STOP", providerPostmaster, token);
        try
        {
            _ = await subscriber.SafePsqlAsync(Database,
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%'", token);
            Assert.IsTrue(await subscriber.PollLogAsync(logpos,
                "timed out waiting for provider \"provider\" to accept the replication connection", token), "apply giving up on the connection");
        }
        finally
        {
            await SignalAsync("-CONT", providerPostmaster, token);
        }

        await AppliesAsync(provider, subscriber, 3, token);

        // A table sync that starts while apply streams, for a table the provider adds, connects to a server that takes
        // connections and never answers, here a listener the provider's interface names now: its connection gives up as
        // apply's does.
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE t2 (id integer PRIMARY KEY)", token);
        _ = await provider.SafePsqlAsync(Database, "CREATE TABLE t2 (id integer PRIMARY KEY); INSERT INTO t2 VALUES (1)", token);
        logpos = subscriber.ReadLog().Length;
        string dsn = await subscriber.SafePsqlAsync(Database,
            "SELECT if_dsn FROM worf.node_interface WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'provider')", token);
        using (TcpListener frozen = new(IPAddress.Loopback, 0))
        {
            frozen.Start();
            List<TcpClient> taken = [];
            using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(token);
            Task taking = Task.Run(async () =>
            {
                try
                {
                    while (true)
                    {
                        taken.Add(await frozen.AcceptTcpClientAsync(stop.Token));
                    }
                }
                catch (OperationCanceledException)
                {
                    // The listener stops.
                }
            }, token);
            try
            {
                _ = await subscriber.SafePsqlAsync(Database, $"""
                    UPDATE worf.node_interface SET if_dsn = 'host=127.0.0.1 port={((IPEndPoint)frozen.LocalEndpoint).Port} dbname={Database} user={SuperUser}'
                    WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'provider')
                    """, token);
                _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 't2', synchronize_data := true)", token);
                Assert.IsTrue(await subscriber.PollLogAsync(logpos, "timed out waiting for provider \"provider\" to list its managed sequences", token),
                    "the table sync giving up on its first call");
            }
            finally
            {
                await stop.CancelAsync();
                await taking;
                _ = await subscriber.SafePsqlAsync(Database,
                    $"UPDATE worf.node_interface SET if_dsn = '{dsn.Replace("'", "''", StringComparison.Ordinal)}' WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'provider')", token);
            }

            foreach (TcpClient client in taken)
            {
                client.Dispose();
            }
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 't2'", token), "t2 synchronized once the provider answers");
        await AppliesAsync(provider, subscriber, 5, token);

        // The subscriber's postmaster stops: the manager can't start the apply worker of the subscription enabled again,
        // nor the exchange worker it starts from time to time, and gives up on the first it waits for. Only a session that
        // was open before can reach the server meanwhile.
        logpos = subscriber.ReadLog().Length;
        string subscriberPostmaster = Postmaster(subscriber);
        await using NpgsqlConnection session = await subscriber.OpenAsync(Database, token);
        await SignalAsync("-STOP", subscriberPostmaster, token);
        try
        {
            foreach (string sql in new[] { "SELECT worf.alter_subscription_disable('sub')", "SELECT worf.alter_subscription_enable('sub')" })
            {
                await using NpgsqlCommand command = new(sql, session);
                _ = await command.ExecuteScalarAsync(token);
            }

            Assert.IsTrue(await subscriber.PollLogAsync(logpos, "worker for subscription \"sub\" at slot", token), "the manager giving up on the worker, naming its subscription");
        }
        finally
        {
            await SignalAsync("-CONT", subscriberPostmaster, token);
        }

        await AppliesAsync(provider, subscriber, 6, token);
    }

    /// <summary>
    /// An apply worker that stops while it copies a subscription's tables starts the copy again, and a connection it
    /// makes for the copy to a provider whose postmaster has stopped gives up after wal_receiver_timeout, naming what it
    /// waited for, rather than wait on it for good; once the provider goes on, the copy completes. Processes are stopped
    /// with signals, which Windows lacks.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public async Task RestartedCopiesTimeOut()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t SELECT g FROM generate_series(1, 1000000) g", token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := true)
            """, token);

        // The apply worker stops while the copy's rows stream, and the provider's postmaster stops, so that the copy the
        // restarted worker starts again connects to a server that takes the connection and never answers.
        Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE query LIKE 'COPY %' AND state = 'active')", token), "the copy streaming");
        int logpos = subscriber.ReadLog().Length;
        string providerPostmaster = Postmaster(provider);
        await SignalAsync("-STOP", providerPostmaster, token);
        try
        {
            _ = await subscriber.SafePsqlAsync(Database,
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%'", token);
            Assert.IsTrue(await subscriber.PollLogAsync(logpos, "timed out waiting for provider \"provider\"", token), "the copy giving up on its call");
        }
        finally
        {
            await SignalAsync("-CONT", providerPostmaster, token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT FROM worf.show_subscription_status() WHERE status = 'replicating')", token), "the subscription replicating");
        Assert.AreEqual("1000000", await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM t", token), "the copied rows");
        await AppliesAsync(provider, subscriber, 1000001, token);
    }

    /// <summary>
    /// With wal_receiver_timeout off, a call apply makes to its provider as it starts, here a connection that a stopped
    /// postmaster never accepts, still gives up, after a minute, rather than wait for good. Processes are stopped with
    /// signals, which Windows lacks.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public async Task ProviderCallsTimeOutWithoutWalReceiverTimeout()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token, "0");
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT FROM worf.show_subscription_status() WHERE status = 'replicating')", token), "the subscription replicating");
        await AppliesAsync(provider, subscriber, 1, token);

        int logpos = subscriber.ReadLog().Length;
        string providerPostmaster = Postmaster(provider);
        await SignalAsync("-STOP", providerPostmaster, token);
        try
        {
            Stopwatch waiting = Stopwatch.StartNew();
            _ = await subscriber.SafePsqlAsync(Database,
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%'", token);
            Assert.IsTrue(await subscriber.PollLogAsync(logpos,
                "timed out waiting for provider \"provider\" to accept the replication connection", token), "apply giving up on the connection");
            Assert.IsGreaterThan(TimeSpan.FromSeconds(55), waiting.Elapsed, "apply's wait for the connection, a minute with wal_receiver_timeout off");
        }
        finally
        {
            await SignalAsync("-CONT", providerPostmaster, token);
        }

        await AppliesAsync(provider, subscriber, 2, token);
    }

    private static async Task AppliesAsync(TestNode provider, TestNode subscriber, int id, CancellationToken token)
    {
        _ = await provider.SafePsqlAsync(Database, $"INSERT INTO t VALUES ({id})", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"SELECT EXISTS (SELECT FROM t WHERE id = {id})", token), $"row {id}");
    }

    private static string Postmaster(TestNode node) => File.ReadLines(Path.Join(node.Cluster.DataDirectory, "postmaster.pid")).First().Trim();

    private static async Task SignalAsync(string signal, string pid, CancellationToken token)
    {
        (int exitCode, _, string error) = await ProcessRunner.RunAsync("kill", [signal, pid], token);
        Assert.AreEqual(0, exitCode, error);
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token, string walReceiverTimeout = "2s")
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            $"wal_receiver_timeout = '{walReceiverTimeout}'",
        ], token);
        try
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER", token);
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION worf", token);
            _ = await node.SafePsqlAsync(Database, $"SELECT * FROM worf.create_node(node_name := '{name}', dsn := '{node.Dsn(Database, SuperUser)}')", token);
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id integer PRIMARY KEY);
                SELECT * FROM worf.replication_set_add_table('default', 't');
                """, token);
        }
        catch
        {
            await node.DisposeAsync();
            throw;
        }

        return node;
    }
}
