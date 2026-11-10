using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Ankus.Postgres;
#if WORF_WINDOWS
using PgSocket = ulong;
#else
using PgSocket = int;
#endif

namespace Worf;

/// <summary>
/// A connection to a neighbor that the exchange worker makes without waiting for it, so that a neighbor that never
/// answers, or answers slowly, holds up none of the worker's calls to the others: the host names libpq would connect to,
/// from the connection string, a service file or the environment, are looked up on other threads, libpq connects to each
/// address found in turn, as libpq does, giving each the connection's <c>connect_timeout</c>, and passing over a name
/// that has none, and the session runs its setup, as the worker
/// looks at the connection's socket between its other work. The connection, once the neighbor has answered, becomes
/// the one the worker calls the neighbor over. libpq's polling, status and result values are part of its stable
/// interface.
/// </summary>
internal sealed unsafe class WorfProbe : IDisposable
{
    private const int WlSocketReadable = 2;
    private const int WlSocketWriteable = 4;
    private const int WlTimeout = 8;
    private const int WlExitOnPmDeath = 32;
    private const int WlSocketConnected = 64;
    private const int WlLatchSet = 1;
    private const uint PgWaitExtension = 0x07000000;

    // PGRES_POLLING_FAILED, READING, WRITING and OK, CONNECTION_OK, CONNECTION_BAD and CONNECTION_STARTED, and
    // PGRES_COMMAND_OK and PGRES_TUPLES_OK.
    private const int PollingFailed = 0;
    private const int PollingReading = 1;
    private const int PollingWriting = 2;
    private const int ConnectionOk = 0;
    private const int ConnectionBad = 1;
    private const int ConnectionStarted = 2;
    private const int CommandOk = 1;
    private const int TuplesOk = 2;

    // PGINVALID_SOCKET: INVALID_SOCKET on Windows, -1 elsewhere.
#if WORF_WINDOWS
    private const PgSocket NoSocket = ulong.MaxValue;
#else
    private const PgSocket NoSocket = -1;
#endif

    private readonly string _conninfo;
    private readonly string _applicationName;
    private readonly string _peer;
    private readonly uint _peerNode;
    private readonly byte[] _setup;
    private readonly List<(string Host, Task<IPAddress[]>? Lookup)> _hosts;
    private readonly string[] _ports;
    private readonly long _connectTimeout;
    private readonly bool _random;
    private readonly List<string> _failures = [];
    private List<(string Host, string Address, string Port)>? _attempts;
    private int _attempt = -1;
    private long _attemptDeadline;
    private void* _conn;
    private int _polling = PollingWriting;
    private bool _sent;
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
    private bool _fd;
#endif

    private WorfProbe(string conninfo, string applicationName, (string Name, uint Node) peer, byte[] setup, long deadline, List<(string Host, Task<IPAddress[]>? Lookup)> hosts,
        string[] ports, long connectTimeout, bool random)
    {
        _conninfo = conninfo;
        _applicationName = applicationName;
        (_peer, _peerNode) = peer;
        _setup = setup;
        _hosts = hosts;
        _ports = ports;
        _connectTimeout = connectTimeout;
        _random = random;
        Deadline = deadline;
    }

    /// <summary>
    /// Gets when the probe runs out of time, as a timestamp.
    /// </summary>
    internal long Deadline { get; }

    /// <summary>
    /// Gets why the probe failed, or null.
    /// </summary>
    internal string? Failure { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the neighbor answered: the connection is made and set up.
    /// </summary>
    internal bool Answered { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the probe has ended, answered or failed.
    /// </summary>
    internal bool Done => Answered || Failure is not null;

    /// <summary>
    /// Starts connecting: looks up the host names libpq would connect to, if the connection gives no addresses, on other
    /// threads, and connects once they're found.
    /// </summary>
    /// <param name="conninfo">The connection string.</param>
    /// <param name="applicationName">The name the server shows for the connection.</param>
    /// <param name="peer">What this server's log calls the server in the notices it sends, as <c>node node_b</c>, and the
    /// node it is.</param>
    /// <param name="setup">The statements the session runs once connected, in one round trip.</param>
    /// <param name="deadline">When the probe runs out of time, as a timestamp.</param>
    /// <returns>The probe.</returns>
    internal static WorfProbe Start(string conninfo, string applicationName, (string Name, uint Node) peer, string setup, long deadline)
    {
        // A host given as an address, or a socket's directory, needs no lookup; libpq looks up the others as it connects,
        // which would hold the worker, those a service file or the environment gives too. Addresses the connection gives
        // stand for its hosts, as libpq takes them.
        List<(string Host, Task<IPAddress[]>? Lookup)> hosts = [];
        string[] ports = [];
        Dictionary<string, string>? options = WorfConnection.EffectiveOptions(conninfo, applicationName);
        if (options is not null)
        {
            string[] names = options.GetValueOrDefault("host") is { Length: > 0 } host ? [.. host.Split(',').Select(each => each.Trim())] : [];
            if (options.GetValueOrDefault("hostaddr") is { Length: > 0 } given)
            {
                string[] addresses = [.. given.Split(',').Select(each => each.Trim())];
                for (int i = 0; i < addresses.Length; i++)
                {
                    hosts.Add((i < names.Length && names[i].Length > 0 ? names[i] + "\0" + addresses[i] : addresses[i], null));
                }
            }
            else
            {
                foreach (string name in names)
                {
                    hosts.Add((name, name.Length == 0 || name[0] is '/' or '@' || IPAddress.TryParse(name, out _) ? null : Task.Run(() => Dns.GetHostAddresses(name))));
                }
            }

            ports = options.GetValueOrDefault("port") is { Length: > 0 } port ? [.. port.Split(',').Select(each => each.Trim())] : [];
        }

        // libpq waits at least 2 seconds for each address when connect_timeout sets a time.
        long connectTimeout = options?.GetValueOrDefault("connect_timeout") is string seconds &&
            int.TryParse(seconds, NumberStyles.Integer, CultureInfo.InvariantCulture, out int timeout) && timeout > 0 ? Math.Max(timeout, 2) * 1_000_000L : 0;
        WorfProbe probe = new(conninfo, applicationName, peer, Worf.ToServerCString(setup), deadline, hosts, ports, connectTimeout,
            options?.GetValueOrDefault("load_balance_hosts") == "random");
        probe.Advance();
        return probe;
    }

    /// <summary>
    /// Takes the probe as far as it goes now, without waiting: looks for the host names' addresses, connects, sends the
    /// setup, and reads its results.
    /// </summary>
    internal void Advance()
    {
        if (_conn is null && !Done)
        {
            if (_hosts.Exists(host => host.Lookup is { IsCompleted: false }))
            {
                return;
            }

            Connect();
        }

        while (_conn is not null && !Done)
        {
            if (!_sent)
            {
                if (_polling is PollingReading or PollingWriting)
                {
                    // An address that doesn't connect within its time gives way to the next, as libpq's own wait for a
                    // connection does with connect_timeout.
                    Follow();
                    if (_attemptDeadline != 0 && Worker.GetCurrentTimestamp() >= _attemptDeadline)
                    {
                        TimedOut();
                        continue;
                    }

                    if (Socket() == NoSocket)
                    {
                        Failure = ErrorMessage() is { Length: > 0 } message ? Failures(message) : "the connection has no socket";
                        return;
                    }

                    if (!Ready(ConnectFlag(), 0))
                    {
                        return;
                    }

                    _polling = ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQconnectPoll"))(_conn);
                    continue;
                }

                // libpq has tried the addresses after the one it failed at as far as it would, and stopped at a server
                // that refused the connection, as it does.
                if (_polling == PollingFailed || Status() != ConnectionOk)
                {
                    Failure = Failures(ErrorMessage());
                    return;
                }

                fixed (byte* setup = _setup)
                {
                    if (((delegate* unmanaged<void*, sbyte*, int>)WorfConnection.LibpqFunction("PQsendQuery"))(_conn, (sbyte*)setup) == 0)
                    {
                        Failure = ErrorMessage();
                        return;
                    }
                }

                _sent = true;
            }

            if (((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQconsumeInput"))(_conn) == 0)
            {
                Failure = ErrorMessage();
                return;
            }

            var busy = (delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQisBusy");
            var getResult = (delegate* unmanaged<void*, void*>)WorfConnection.LibpqFunction("PQgetResult");
            var resultStatus = (delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQresultStatus");
            var resultError = (delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction("PQresultErrorMessage");
            var clear = (delegate* unmanaged<void*, void>)WorfConnection.LibpqFunction("PQclear");
            string? failed = null;
            while (busy(_conn) == 0)
            {
                void* result = getResult(_conn);
                if (result is null)
                {
                    if (failed is not null)
                    {
                        Failure = failed;
                    }
                    else
                    {
                        Answered = true;
                    }

                    return;
                }

                if (resultStatus(result) is not (CommandOk or TuplesOk))
                {
                    failed ??= Worf.FromServer(resultError(result)).TrimEnd();
                }

                clear(result);
            }

            if (failed is not null)
            {
                Failure = failed;
            }

            return;
        }
    }

    /// <summary>
    /// Waits up to a time for the probe's socket to be ready for its next step, or for the latch, or, while host names
    /// are looked up, for a moment.
    /// </summary>
    /// <param name="milliseconds">The time.</param>
    /// <returns>Whether the latch was set, which the wait resets.</returns>
    internal bool Wait(long milliseconds)
    {
        int flag = 0;
        PgSocket socket = NoSocket;
        if (_conn is null)
        {
            milliseconds = Math.Min(milliseconds, 20);
        }
        else if (Socket() is PgSocket ready && ready != NoSocket)
        {
            flag = _sent ? WlSocketReadable : ConnectFlag();
            socket = ready;

            // An address being connected to is given up at its connect_timeout.
            if (!_sent && _attemptDeadline != 0)
            {
                milliseconds = Math.Min(milliseconds, Math.Max((_attemptDeadline - Worker.GetCurrentTimestamp() + 999) / 1000, 0));
            }
        }
        else if (_sent)
        {
            Failure ??= "the connection has no socket";
            return false;
        }
        else
        {
            return false;
        }

        int events = NativeMethods.WaitLatchOrSocket(NativeGlobals.MyLatch, WlExitOnPmDeath | WlLatchSet | WlTimeout | flag, socket,
            Math.Max(milliseconds, 0), PgWaitExtension);
        if ((events & WlLatchSet) == 0)
        {
            return false;
        }

        NativeMethods.ResetLatch(NativeGlobals.MyLatch);
        return true;
    }

    /// <summary>
    /// Takes the connection the probe made, once the neighbor answered.
    /// </summary>
    /// <returns>The connection, which closes the libpq connection as it's disposed.</returns>
    internal WorfConnection Take()
    {
        void* conn = _conn;
        _conn = null;
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
        _fd = false;
#endif
        return WorfConnection.FromLibpq(conn);
    }

    /// <summary>
    /// Closes the connection, unless it was taken.
    /// </summary>
    public void Dispose()
    {
        if (_conn is null)
        {
            return;
        }

        WorfNotices.Forget(_conn);
        ((delegate* unmanaged<void*, void>)WorfConnection.LibpqFunction("PQfinish"))(_conn);
        _conn = null;
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
        if (_fd)
        {
            NativeMethods.ReleaseExternalFD();
        }

        _fd = false;
#endif
    }

    // Lists the addresses the host names' lookups found, each with its name and port, passing over a name that has none,
    // in random order when the connection asks libpq to balance its hosts, and connects to the first.
    private void Connect()
    {
        int[] order = [.. Enumerable.Range(0, _hosts.Count)];
        if (_random)
        {
            Random.Shared.Shuffle(order);
        }

        _attempts = [];
        foreach (int index in order)
        {
            (string host, Task<IPAddress[]>? lookup) = _hosts[index];
            string port = _ports.Length == 1 ? _ports[0] : index < _ports.Length ? _ports[index] : "";
            if (lookup is null)
            {
                // A host given with its address, as hostaddr gives it, is the name and the address apart.
                int split = host.IndexOf('\0', StringComparison.Ordinal);
                _attempts.Add(split >= 0 ? (host[..split], host[(split + 1)..], port) : (host, IPAddress.TryParse(host, out _) ? host : "", port));
            }
            else if (lookup.IsCompletedSuccessfully && lookup.Result.Length > 0)
            {
                IPAddress[] found = [.. lookup.Result];
                if (_random)
                {
                    Random.Shared.Shuffle(found);
                }

                _attempts.AddRange(found.Select(address => (host, address.ToString(), port)));
            }
            else
            {
                _failures.Add($"could not translate host name \"{host}\" to address: {(lookup.Exception?.InnerException as SocketException)?.Message ?? "no address found"}");
            }
        }

        // A connection string that names no host connects as libpq's defaults have it.
        if (_hosts.Count == 0)
        {
            _attempts.Add(("", "", ""));
        }

        // From PostgreSQL 18, libpqwalreceiver counts the connections it closes against the server's external files, as
        // it counts those it opens.
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
        if (!NativeMethods.AcquireExternalFD())
        {
            Failure = "could not connect to the server: too many open files";
            return;
        }

        _fd = true;
#endif
        Next();
    }

    // Connects to the addresses from the next one on, which libpq tries in turn, moving on as it does: after one that
    // doesn't take the connection, or a server that is starting or isn't of the session's attributes, but not after one
    // that refuses the connection, as for a role it doesn't have. A connection that libpq finds bad at once, as for an
    // option it doesn't know, fails.
    private void Next()
    {
        _attempt++;
        if (_attempts is null || _attempt >= _attempts.Count)
        {
            Failure = Failures(null);
            return;
        }

        var start = (delegate* unmanaged<sbyte**, sbyte**, int, void*>)WorfConnection.LibpqFunction("PQconnectStartParams");
        List<(string Host, string Address, string Port)> rest = _attempts[_attempt..];
        byte[] info = Worf.ToServerCString(_conninfo);
        byte[] application = Worf.ToServerCString(_applicationName);
        byte[] hostValue = Worf.ToServerCString(string.Join(',', rest.Select(attempt => attempt.Host)));
        byte[] hostaddr = Worf.ToServerCString(string.Join(',', rest.Select(attempt => attempt.Address)));
        byte[] portValue = Worf.ToServerCString(string.Join(',', rest.Select(attempt => attempt.Port)));
        bool listed = _hosts.Count > 0;
        bool ported = listed && _ports.Length > 0;
        fixed (byte* dbname = "dbname\0"u8, fallback = "fallback_application_name\0"u8, hostKey = "host\0"u8, hostaddrKey = "hostaddr\0"u8,
            portKey = "port\0"u8, balanceKey = "load_balance_hosts\0"u8, balanceValue = "disable\0"u8, infoAddress = info, applicationAddress = application,
            hostAddress = hostValue, hostaddrValue = hostaddr, portAddress = portValue)
        {
            // The hosts, their addresses and ports come after the connection string, whose values they take the place
            // of, in the order the probe has them in.
            sbyte** keys = stackalloc sbyte*[]
            {
                (sbyte*)dbname, (sbyte*)fallback, listed ? (sbyte*)hostKey : null, listed ? (sbyte*)hostaddrKey : null, ported ? (sbyte*)portKey : null,
                _random ? (sbyte*)balanceKey : null, null,
            };
            sbyte** values = stackalloc sbyte*[]
            {
                (sbyte*)infoAddress, (sbyte*)applicationAddress, (sbyte*)hostAddress, (sbyte*)hostaddrValue, (sbyte*)portAddress, (sbyte*)balanceValue, null,
            };
            if (!ported)
            {
                keys[4] = keys[5];
                values[4] = values[5];
                keys[5] = null;
            }

            _conn = start(keys, values, 1);
        }

        if (_conn is null)
        {
            Failure = "out of memory";
            return;
        }

        WorfNotices.Route(_conn, _peer, _peerNode);
        _polling = PollingWriting;
        StartAttempt();
        if (Status() == ConnectionBad)
        {
            Failure = Failures(ErrorMessage());
        }
    }

    // Gives an address its time: the connection's connect_timeout, or with none, or a longer one, its share of what the
    // probe has left, divided among the addresses not yet tried, so that one that never answers leaves the others time.
    private void StartAttempt()
    {
        long now = Worker.GetCurrentTimestamp();
        int left = Math.Max(_attempts!.Count - _attempt, 1);
        long share = Math.Max((Deadline - now) / left, 0);
        _attemptDeadline = now + (_connectTimeout > 0 ? Math.Min(_connectTimeout, left > 1 ? share : _connectTimeout) : left > 1 ? share : 0);
        if (_connectTimeout <= 0 && left == 1)
        {
            _attemptDeadline = 0;
        }
    }

    // Whether libpq connects to an address now, by the host, address and port it gives: the host it names, or the
    // address it was given as, the address it reached, where both are known, and the port, where one was given.
    private static bool IsConnecting((string Host, string Address, string Port) attempt, string host, string address, string port)
    {
        if (attempt.Host != host && attempt.Address != host)
        {
            return false;
        }

        if (address.Length > 0 && attempt.Address.Length > 0 && attempt.Address != address)
        {
            return false;
        }

        return attempt.Port.Length == 0 || attempt.Port == port;
    }

    // Follows libpq as it moves on to a later address, by the host, address and port it connects to now.
    private void Follow()
    {
        string host = Text("PQhost");
        string address = Text("PQhostaddr");
        string port = Text("PQport");
        for (int i = _attempt; i < _attempts!.Count; i++)
        {
            if (IsConnecting(_attempts[i], host, address, port))
            {
                if (i != _attempt)
                {
                    _attempt = i;
                    StartAttempt();
                }

                return;
            }
        }

        string Text(string function)
        {
            sbyte* value = ((delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction(function))(_conn);
            return value is null ? "" : Worf.FromServer(value);
        }
    }

    // Gives up the address being connected to at its time, and connects to those after it.
    private void TimedOut()
    {
        _failures.Add(AttemptFailure("timeout expired"));
        WorfNotices.Forget(_conn);
        ((delegate* unmanaged<void*, void>)WorfConnection.LibpqFunction("PQfinish"))(_conn);
        _conn = null;
        Next();
    }

    // The probe's failure: the names it found no address for, the addresses that ran out of time, and libpq's own.
    private string Failures(string? last)
        => string.Join('\n', last is null ? _failures : [.. _failures, last.TrimEnd()]) is { Length: > 0 } all ? all : "could not connect to the server";

    // A failure of the address being connected to, worded as libpq words its own.
    private string AttemptFailure(string reason)
    {
        (string host, string address, string port) = _attempts is not null && _attempt >= 0 && _attempt < _attempts.Count ? _attempts[_attempt] : ("", "", "");
        string at = address.Length > 0 && address != host ? $"\"{host}\" ({address})" : $"\"{host}\"";
        return port.Length > 0 ? $"connection to server at {at}, port {port} failed: {reason}" : $"connection to server at {at} failed: {reason}";
    }

    // What the connection waits for: to read or to write, or on Windows, while the connect itself is under way, for it
    // to finish, as Windows reports a refused connect as writable, which would fail it rather than try the next address.
    private int ConnectFlag()
        => _polling == PollingReading ? WlSocketReadable
            : OperatingSystem.IsWindows() && Status() == ConnectionStarted ? WlSocketConnected : WlSocketWriteable;

    // Whether the socket is ready for what the probe waits for, now; a connection without a socket has failed.
    private bool Ready(int flag, long milliseconds)
    {
        PgSocket socket = Socket();
        if (socket == NoSocket)
        {
            Failure ??= ErrorMessage() is { Length: > 0 } message ? message : "the connection has no socket";
            return false;
        }

        return (NativeMethods.WaitLatchOrSocket(null, WlExitOnPmDeath | WlTimeout | flag, socket, milliseconds, PgWaitExtension) & flag) != 0;
    }

    private int Status() => ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQstatus"))(_conn);

    private PgSocket Socket()
    {
        int descriptor = ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQsocket"))(_conn);
        return descriptor < 0 ? NoSocket : WorfConnection.ToSocket(descriptor);
    }

    private string ErrorMessage()
        => Worf.FromServer(((delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction("PQerrorMessage"))(_conn)).TrimEnd();
}
