using System.Buffers.Binary;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Npgsql;
using Worf.CreateSubscriber;

namespace Worf.Tests;

/// <summary>
/// Tests that <c>worf_create_subscriber</c> connects as libpq does.
/// </summary>
[TestClass]
public sealed class LibpqTests
{
    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// connect_timeout bounds each server on its own, so a server that never answers leaves the next its own time,
    /// and every server's failure is reported.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task EachServerGetsItsOwnConnectTimeout()
    {
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(TestContext.CancellationToken);
        TcpListener silent = Listen();
        TcpListener rejecting = Listen();
        Task rejections = RejectAsync(rejecting, stop.Token);
        try
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            Assert.AreEqual(PingStatus.Reject, Libpq.Ping(Conninfo([Port(silent), Port(rejecting)], "connect_timeout=2")),
                "the second server is tried after the first times out");
            Assert.IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(1.9), elapsed.Elapsed, "the first server had its 2 seconds");

            elapsed.Restart();
            NpgsqlException error = Assert.ThrowsExactly<NpgsqlException>(() => Libpq.Connect(Conninfo([Port(silent), Port(silent)], "connect_timeout=2")));
            Assert.IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(3.9), elapsed.Elapsed, "each server had its 2 seconds");
            string server = $"connection to server at \"127.0.0.1\", port {Port(silent)} failed: timeout expired";
            Assert.AreEqual($"{server}\n{server}", error.Message);
        }
        finally
        {
            await stop.CancelAsync();
            await StopAsync(rejecting, rejections);
            silent.Stop();
        }
    }

    /// <summary>
    /// A connect_timeout longer than Npgsql or a timer takes is libpq's to take too, so a ping still reaches the
    /// servers rather than reporting no attempt.
    /// </summary>
    /// <param name="timeout">The timeout.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("2000")]
    [DataRow("4294968")]
    [DataRow("2147483647")]
    public async Task LongConnectTimeoutsStillConnect(string timeout)
    {
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(TestContext.CancellationToken);
        TcpListener rejecting = Listen();
        Task rejections = RejectAsync(rejecting, stop.Token);
        try
        {
            Assert.AreEqual(PingStatus.Reject, Libpq.Ping(Conninfo([RefusingPort(), Port(rejecting)], $"connect_timeout={timeout}")));
        }
        finally
        {
            await stop.CancelAsync();
            await StopAsync(rejecting, rejections);
        }
    }

    /// <summary>
    /// Options libpq can't parse make no attempt, and the ones it can are taken as it takes them.
    /// </summary>
    /// <param name="options">The options.</param>
    /// <param name="expected">The ping's result for a server that refuses connections.</param>
    [TestMethod]
    [DataRow("connect_timeout=abc", nameof(PingStatus.NoAttempt))]
    [DataRow("connect_timeout=1.5", nameof(PingStatus.NoAttempt))]
    [DataRow("connect_timeout=2147483648", nameof(PingStatus.NoAttempt))]
    [DataRow("connect_timeout=' 3 '", nameof(PingStatus.NoResponse))]
    [DataRow("connect_timeout=-1", nameof(PingStatus.NoResponse))]
    [DataRow("target_session_attrs=master", nameof(PingStatus.NoAttempt))]
    [DataRow("target_session_attrs=prefer-standby", nameof(PingStatus.NoResponse))]
    public void ParsesOptionsAsLibpqDoes(string options, string expected)
    {
        Assert.AreEqual(expected, Libpq.Ping(Conninfo([RefusingPort()], options)).ToString());
    }

    /// <summary>
    /// A connection that requires TLS gets it from a server that offers it.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RequiredTlsIsNegotiated()
    {
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(TestContext.CancellationToken);
        using RSA key = RSA.Create(2048);
        CertificateRequest request = new("CN=127.0.0.1", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using X509Certificate2 created = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        // Windows' TLS takes a certificate's key only from a store or a PKCS #12 file, not one in memory.
        using X509Certificate2 certificate = X509CertificateLoader.LoadPkcs12(created.Export(X509ContentType.Pkcs12), null);
        TcpListener rejecting = Listen();
        Task rejections = RejectAsync(rejecting, stop.Token, certificate);
        try
        {
            Assert.AreEqual(PingStatus.Reject, Libpq.Ping(Conninfo([Port(rejecting)], "sslmode=require connect_timeout=10")));
        }
        finally
        {
            await stop.CancelAsync();
            await StopAsync(rejecting, rejections);
        }
    }

    /// <summary>
    /// A list of ports has one port for every host, or one for all of them.
    /// </summary>
    [TestMethod]
    public void MatchesPortsToHosts()
    {
        Assert.AreEqual(PingStatus.NoAttempt, Libpq.Ping("host=127.0.0.1,127.0.0.1 port=1,2,3"));
        Assert.AreEqual(PingStatus.NoAttempt, Libpq.Ping("host=a,b hostaddr=127.0.0.1 port=1"));
        Assert.AreEqual(PingStatus.NoAttempt, Libpq.Ping("host=127.0.0.1 port=65536"));

        _ = Libpq.ToNpgsql("host=a,b port=5433", out List<(string Host, int Port)> servers, out _, out _);
        Assert.AreSequenceEqual<(string Host, int Port)>([("a", 5433), ("b", 5433)], servers);
        _ = Libpq.ToNpgsql("host=a,,c port=1,,3", out servers, out _, out _);
        Assert.AreEqual(("a", 1), servers[0]);
        Assert.AreEqual(5432, servers[1].Port, "an empty port is the default");
        Assert.AreEqual(("c", 3), servers[2]);
        _ = Libpq.ToNpgsql("host=a, hostaddr=127.0.0.1,127.0.0.2", out servers, out _, out _);
        Assert.AreSequenceEqual<(string Host, int Port)>([("a", 5432), ("127.0.0.2", 5432)], servers, "a host's name, or else its address");
    }

    /// <summary>
    /// target_session_attrs passes over servers whose sessions aren't the kind wanted, in libpq's words, and
    /// prefer-standby takes a primary when there is no standby.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TargetSessionAttrsChooseTheServer()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("primary", [], token);
        TcpListener silent = Listen();
        try
        {
            string primary = $"{node.Dsn("postgres", "postgres")} connect_timeout=2";
            string[] suitable = ["any", "read-write", "primary", "prefer-standby"];
            foreach (string attrs in suitable)
            {
                using NpgsqlConnection connection = Libpq.Connect($"{primary} target_session_attrs={attrs}");
                Assert.AreEqual(ConnectionState.Open, connection.State, attrs);
            }

            string server = $"connection to server at \"127.0.0.1\", port {node.Cluster.Port.ToString(CultureInfo.InvariantCulture)} failed";
            NpgsqlException error = Assert.ThrowsExactly<NpgsqlException>(() => Libpq.Connect($"{primary} target_session_attrs=standby"));
            Assert.AreEqual($"{server}: server is not in hot standby mode", error.Message);
            error = Assert.ThrowsExactly<NpgsqlException>(() => Libpq.Connect($"{primary} target_session_attrs=read-only"));
            Assert.AreEqual($"{server}: session is not read-only", error.Message);
            Assert.AreEqual(PingStatus.Ok, Libpq.Ping($"{primary} target_session_attrs=standby"), "the server answered");


            // A server that never answers is passed over after its timeout.
            using NpgsqlConnection second = Libpq.Connect(
                $"host=127.0.0.1,127.0.0.1 port={Port(silent)},{node.Cluster.Port.ToString(CultureInfo.InvariantCulture)} " +
                "dbname=postgres user=postgres connect_timeout=2 target_session_attrs=read-write");
            Assert.AreEqual(ConnectionState.Open, second.State);

        }
        finally
        {
            silent.Stop();
        }
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
    /// Finds a port nothing listens on, so that connecting to it is refused.
    /// </summary>
    private static string RefusingPort()
    {
        TcpListener listener = Listen();
        string port = Port(listener);
        listener.Stop();
        return port;
    }

    private static string Conninfo(string[] ports, string options)
        => $"host={string.Join(',', ports.Select(static _ => "127.0.0.1"))} port={string.Join(',', ports)} " +
            $"dbname=postgres user=postgres {options}";

    /// <summary>
    /// Answers every connection as a server that is starting up does, over TLS when it has a certificate.
    /// </summary>
    private static async Task RejectAsync(TcpListener listener, CancellationToken cancellationToken, X509Certificate2? certificate = null)
    {
        byte[] fields = Encoding.ASCII.GetBytes("SFATAL\0VFATAL\0C57P03\0Mthe database system is starting up\0\0");
        byte[] error = new byte[5 + fields.Length];
        error[0] = (byte)'E';
        BinaryPrimitives.WriteInt32BigEndian(error.AsSpan(1), 4 + fields.Length);
        fields.CopyTo(error, 5);

        while (!cancellationToken.IsCancellationRequested)
        {
            using TcpClient client = await listener.AcceptTcpClientAsync(cancellationToken);
            await using NetworkStream connection = client.GetStream();
            try
            {
                // A TLS request is accepted when there's a certificate, and the startup packet answered with the error.
                if (await ReadRequestsAsync(connection, certificate is not null, cancellationToken))
                {
                    await using SslStream tls = new(connection, leaveInnerStreamOpen: true);
                    await tls.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificate = certificate }, cancellationToken);
                    _ = await ReadRequestsAsync(tls, false, cancellationToken);
                    await tls.WriteAsync(error, cancellationToken);
                }
                else
                {
                    await connection.WriteAsync(error, cancellationToken);
                }
            }
            catch (Exception failure) when (failure is IOException or EndOfStreamException or AuthenticationException)
            {
                // The client went away.
            }
        }
    }

    /// <summary>
    /// Reads a client's requests up to its startup packet, declining encryption, or until it asks for TLS when that's
    /// accepted.
    /// </summary>
    /// <returns>Whether the client asked for TLS and was told to go ahead.</returns>
    private static async Task<bool> ReadRequestsAsync(Stream stream, bool acceptTls, CancellationToken cancellationToken)
    {
        while (true)
        {
            byte[] length = new byte[4];
            await stream.ReadExactlyAsync(length, cancellationToken);
            byte[] packet = new byte[BinaryPrimitives.ReadInt32BigEndian(length) - 4];
            await stream.ReadExactlyAsync(packet, cancellationToken);
            int code = BinaryPrimitives.ReadInt32BigEndian(packet);
            if (code == 80877103 && acceptTls)
            {
                await stream.WriteAsync("S"u8.ToArray(), cancellationToken);
                return true;
            }

            if (code is not (80877103 or 80877104))
            {
                return false;
            }

            await stream.WriteAsync("N"u8.ToArray(), cancellationToken);
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
