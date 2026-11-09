using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Worf.Tests;

/// <summary>
/// A TCP relay to a server, for tests, which reads each connection's startup packet and the queries its client sends: it
/// can hold the WAL that a standby's walreceiver streams once the replication handshake is through, so that the standby
/// streams toward its restore point and never reaches it, hold every connection made after the walreceiver
/// disconnects, so that the tool's next connection to the provider waits however the test process is scheduled, and
/// hold every query after a connection's first, so that a connection is made and set up but its calls are never
/// answered. It keeps the text of the queries it passes. A connection that asks for SSL or GSS encryption is told the
/// relay has none.
/// </summary>
internal sealed class ProviderRelay : IAsyncDisposable
{
    // The protocol's SSLRequest and GSSENCRequest codes.
    private const int SslRequest = 80877103;
    private const int GssEncRequest = 80877104;

    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly int _target;
    private readonly bool _holdWal;
    private readonly bool _holdAfterWalreceiver;
    private readonly bool _holdCalls;
    private readonly CancellationTokenSource _stop = new();
    private readonly CancellationTokenSource _held = new();
    private readonly List<TcpClient> _clients = [];
    private readonly Task _accepting;
    private volatile bool _holding;

    /// <summary>
    /// Initializes a new instance of the <see cref="ProviderRelay"/> class, listening on a port of its own.
    /// </summary>
    /// <param name="target">The provider's port, on 127.0.0.1.</param>
    /// <param name="holdWal">Whether a walreceiver's connection stops passing the provider's messages once its
    /// replication starts.</param>
    /// <param name="holdAfterWalreceiver">Whether every connection made after a walreceiver's ends is held, answering
    /// nothing.</param>
    /// <param name="holdCalls">Whether each connection's queries after its first are held, never reaching the
    /// server.</param>
    internal ProviderRelay(int target, bool holdWal = false, bool holdAfterWalreceiver = false, bool holdCalls = false)
    {
        _target = target;
        _holdWal = holdWal;
        _holdAfterWalreceiver = holdAfterWalreceiver;
        _holdCalls = holdCalls;
        _listener.Start();
        _accepting = AcceptAsync();
    }

    /// <summary>
    /// Gets the port the relay listens on, on 127.0.0.1.
    /// </summary>
    internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    /// <summary>
    /// Gets a task that completes once a walreceiver's replication has started, whose WAL the relay then holds.
    /// </summary>
    internal TaskCompletionSource Streaming { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Gets a task that completes once the relay holds a connection made after a walreceiver's ended.
    /// </summary>
    internal TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Gets the text of the simple queries the relay has passed, in the order they came.
    /// </summary>
    internal ConcurrentQueue<string> Queries { get; } = new();

    /// <summary>
    /// Stops holding connections, closing those it held, and passes the ones made from now on.
    /// </summary>
    internal void Pass()
    {
        _holding = false;
        _held.Cancel();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        lock (_clients)
        {
            foreach (TcpClient client in _clients)
            {
                client.Dispose();
            }
        }

        try
        {
            await _accepting;
        }
        catch (Exception error) when (error is OperationCanceledException or SocketException or ObjectDisposedException)
        {
            // The listener stopped.
        }

        _stop.Dispose();
        _held.Dispose();
    }

    private static async Task<byte[]> ReadExactlyAsync(NetworkStream stream, int count, CancellationToken token)
    {
        byte[] buffer = new byte[count];
        await stream.ReadExactlyAsync(buffer, token);
        return buffer;
    }

    // Reads the startup packet, answering a request for encryption that comes first with 'N'.
    private static async Task<byte[]> ReadStartupAsync(NetworkStream client, CancellationToken token)
    {
        while (true)
        {
            byte[] length = await ReadExactlyAsync(client, 4, token);
            byte[] body = await ReadExactlyAsync(client, BinaryPrimitives.ReadInt32BigEndian(length) - 4, token);
            int code = BinaryPrimitives.ReadInt32BigEndian(body);
            if (code is SslRequest or GssEncRequest)
            {
                await client.WriteAsync("N"u8.ToArray(), token);
                continue;
            }

            return [.. length, .. body];
        }
    }

    // The startup packet's parameters, after its length and protocol version.
    private static Dictionary<string, string> Parameters(byte[] startup)
    {
        Dictionary<string, string> parameters = [];
        string[] fields = Encoding.UTF8.GetString(startup, 8, startup.Length - 8).Split('\0');
        for (int i = 0; i + 1 < fields.Length && fields[i].Length > 0; i += 2)
        {
            parameters[fields[i]] = fields[i + 1];
        }

        return parameters;
    }

    private static async Task PumpAsync(NetworkStream from, NetworkStream to, CancellationToken token)
    {
        byte[] buffer = new byte[65536];
        int read;
        while ((read = await from.ReadAsync(buffer, token)) > 0)
        {
            await to.WriteAsync(buffer.AsMemory(0, read), token);
        }
    }

    private async Task AcceptAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            TcpClient client = await _listener.AcceptTcpClientAsync(_stop.Token);
            lock (_clients)
            {
                _clients.Add(client);
            }

            _ = Task.Run(() => RelayAsync(client));
        }
    }

    private async Task RelayAsync(TcpClient near)
    {
        CancellationToken token = _stop.Token;
        bool walreceiver = false;
        try
        {
            using (near)
            {
                if (_holding)
                {
                    // Held connections close once the relay passes connections again.
                    using CancellationTokenSource held = CancellationTokenSource.CreateLinkedTokenSource(token, _held.Token);
                    _ = Held.TrySetResult();
                    await Task.Delay(Timeout.Infinite, held.Token);
                }

                NetworkStream client = near.GetStream();
                byte[] startup = await ReadStartupAsync(client, token);
                Dictionary<string, string> parameters = Parameters(startup);
                walreceiver = parameters.ContainsKey("replication") && parameters.GetValueOrDefault("application_name") != "pg_basebackup";
                using TcpClient far = new();
                await far.ConnectAsync(IPAddress.Loopback, _target, token);
                NetworkStream server = far.GetStream();
                await server.WriteAsync(startup, token);
                Task up = walreceiver ? PumpAsync(client, server, token) : PassQueriesAsync(client, server, token);
                Task down = walreceiver && _holdWal ? PassUntilReplicationAsync(server, client, token) : PumpAsync(server, client, token);
                _ = await Task.WhenAny(up, down);
            }
        }
        catch (Exception error) when (error is IOException or SocketException or OperationCanceledException or ObjectDisposedException or EndOfStreamException)
        {
            // A link ends as either side closes, or as the relay stops.
        }
        finally
        {
            if (walreceiver && _holdAfterWalreceiver)
            {
                _holding = true;
            }
        }
    }

    // Passes a client's messages, keeping its queries' text, and holding those after its first query when it holds calls.
    private async Task PassQueriesAsync(NetworkStream client, NetworkStream server, CancellationToken token)
    {
        int queries = 0;
        while (true)
        {
            byte[] header = await ReadExactlyAsync(client, 5, token);
            byte[] body = await ReadExactlyAsync(client, BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1)) - 4, token);
            if (header[0] == (byte)'Q')
            {
                if (_holdCalls && queries++ > 0)
                {
                    await Task.Delay(Timeout.Infinite, token);
                }

                Queries.Enqueue(Encoding.UTF8.GetString(body, 0, Math.Max(body.Length - 1, 0)));
            }

            byte[] message = [.. header, .. body];
            await server.WriteAsync(message, token);
        }
    }

    // Passes the provider's messages until CopyBothResponse, which starts replication, and then holds the rest.
    private async Task PassUntilReplicationAsync(NetworkStream server, NetworkStream client, CancellationToken token)
    {
        while (true)
        {
            byte[] header = await ReadExactlyAsync(server, 5, token);
            byte[] body = await ReadExactlyAsync(server, BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1)) - 4, token);
            byte[] message = [.. header, .. body];
            await client.WriteAsync(message, token);
            if (header[0] == (byte)'W')
            {
                _ = Streaming.TrySetResult();
                await Task.Delay(Timeout.Infinite, token);
            }
        }
    }
}
