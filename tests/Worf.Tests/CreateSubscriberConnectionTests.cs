using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Worf.Tests;

/// <summary>
/// Tests that <c>worf_create_subscriber</c> connects through libpq, so that a connection string means to it what it
/// means to every other PostgreSQL program.
/// </summary>
[TestClass]
public sealed class CreateSubscriberConnectionTests
{
    private const string StartingUp = "the database system is starting up";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// hostaddr, service files, each host's own connect_timeout and the encryption a connection requires work as in
    /// libpq, against a server that answers every session as one starting up does.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ConnectsAsLibpqDoes()
    {
        CancellationToken token = TestContext.CancellationToken;
        using TemporaryDirectory work = new($"worf-create-subscriber-libpq-{Guid.NewGuid():N}");
        string command = await CreateSubscriberCommand.InstallAsync(await TestInstallations.CurrentAsync(token), Path.Join(work.FullName, "bin"));
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(token);
        TcpListener silent = Listen();
        TcpListener rejecting = Listen();
        Task rejections = RejectAsync(rejecting, stop.Token);
        try
        {
            string port = Port(rejecting);

            // hostaddr gives the address, and host only names the server, so a name that resolves to nothing still
            // reaches it.
            Assert.Contains(StartingUp, await ConnectAsync(command, work.FullName, $"host=create-subscriber.invalid hostaddr=127.0.0.1 port={port}"),
                "hostaddr");

            // A service file gives the server.
            string services = Path.Join(work.FullName, "pg_service.conf");
            await File.WriteAllTextAsync(services, $"[provider]\nhost=127.0.0.1\nport={port}\n", token);
            Assert.Contains(StartingUp, await ConnectAsync(command, work.FullName, "service=provider", new Dictionary<string, string> { ["PGSERVICEFILE"] = services }),
                "service");

            // Each host gets connect_timeout to itself, so one that never answers leaves the next its own time.
            Stopwatch elapsed = Stopwatch.StartNew();
            Assert.Contains(StartingUp, await ConnectAsync(command, work.FullName, $"host=127.0.0.1,127.0.0.1 port={Port(silent)},{port} connect_timeout=2"),
                "the second host");
            Assert.IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(1.9), elapsed.Elapsed, "the first host had its 2 seconds");

            // Encryption the connection requires and the server doesn't offer fails it before a session is asked for.
            string[] requirements = ["sslmode=require", "gssencmode=require"];
            foreach (string requirement in requirements)
            {
                Assert.DoesNotContain(StartingUp, await ConnectAsync(command, work.FullName, $"host=127.0.0.1 port={port} {requirement}"), requirement);
            }
        }
        finally
        {
            await stop.CancelAsync();
            await StopAsync(rejecting, rejections);
            silent.Stop();
        }
    }

    /// <summary>
    /// Runs the command with a provider connection string, which it connects to first, and returns why that failed.
    /// </summary>
    private async Task<string> ConnectAsync(string command, string work, string providerDsn,
        IReadOnlyDictionary<string, string>? environment = null)
    {
        (int exitCode, _, string stderr) = await CreateSubscriberCommand.RunAsync(command,
            ["-D", Path.Join(work, "data"), "--subscriber-name", "subscriber", "--provider-dsn", $"{providerDsn} dbname=postgres",
                "--subscriber-dsn", "dbname=postgres"],
            work, environment, TestContext.CancellationToken);
        Assert.AreNotEqual(0, exitCode, stderr);
        Assert.Contains("Connection to database failed: ", stderr);
        return stderr;
    }

    private static TcpListener Listen()
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        return listener;
    }

    private static string Port(TcpListener listener)
        => ((IPEndPoint)listener.LocalEndpoint).Port.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Answers every connection as a server that is starting up does, declining encryption.
    /// </summary>
    private static async Task RejectAsync(TcpListener listener, CancellationToken cancellationToken)
    {
        byte[] fields = Encoding.ASCII.GetBytes($"SFATAL\0VFATAL\0C57P03\0M{StartingUp}\0\0");
        byte[] error = new byte[5 + fields.Length];
        error[0] = (byte)'E';
        BinaryPrimitives.WriteInt32BigEndian(error.AsSpan(1), 4 + fields.Length);
        fields.CopyTo(error, 5);

        while (!cancellationToken.IsCancellationRequested)
        {
            using TcpClient client = await listener.AcceptTcpClientAsync(cancellationToken);
            await using NetworkStream stream = client.GetStream();
            try
            {
                // Encryption requests are declined, and the startup packet answered with the error.
                while (true)
                {
                    byte[] length = new byte[4];
                    await stream.ReadExactlyAsync(length, cancellationToken);
                    byte[] packet = new byte[BinaryPrimitives.ReadInt32BigEndian(length) - 4];
                    await stream.ReadExactlyAsync(packet, cancellationToken);
                    if (BinaryPrimitives.ReadInt32BigEndian(packet) is not (80877103 or 80877104))
                    {
                        break;
                    }

                    await stream.WriteAsync("N"u8.ToArray(), cancellationToken);
                }

                await stream.WriteAsync(error, cancellationToken);
            }
            catch (Exception failure) when (failure is IOException or EndOfStreamException)
            {
                // The client went away.
            }
        }
    }

    private static async Task StopAsync(TcpListener listener, Task server)
    {
        listener.Stop();
        try
        {
            await server;
        }
        catch (OperationCanceledException)
        {
            // Stopped while waiting for a connection.
        }
    }
}
