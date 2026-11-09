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
/// from the connection string, a service file or the environment, are looked up on other threads, libpq connects to every
/// address found, and the session runs its setup, as the worker
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
    private readonly byte[] _setup;
    private readonly List<(string Host, Task<IPAddress[]>? Lookup)> _hosts;
    private readonly string[] _ports;
    private void* _conn;
    private int _polling = PollingWriting;
    private bool _sent;
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
    private bool _fd;
#endif

    private WorfProbe(string conninfo, string applicationName, string peer, byte[] setup, long deadline, List<(string Host, Task<IPAddress[]>? Lookup)> hosts,
        string[] ports)
    {
        _conninfo = conninfo;
        _applicationName = applicationName;
        _peer = peer;
        _setup = setup;
        _hosts = hosts;
        _ports = ports;
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
    /// <param name="peer">What this server's log calls the server in the notices it sends, as <c>node node_b</c>.</param>
    /// <param name="setup">The statements the session runs once connected, in one round trip.</param>
    /// <param name="deadline">When the probe runs out of time, as a timestamp.</param>
    /// <returns>The probe.</returns>
    internal static WorfProbe Start(string conninfo, string applicationName, string peer, string setup, long deadline)
    {
        // A host given as an address, or a socket's directory, needs no lookup; libpq looks up the others as it connects,
        // which would hold the worker, those a service file or the environment gives too.
        List<(string Host, Task<IPAddress[]>? Lookup)> hosts = [];
        string[] ports = [];
        if (EffectiveOptions(conninfo, applicationName) is Dictionary<string, string> options && options.GetValueOrDefault("hostaddr") is not { Length: > 0 } &&
            options.GetValueOrDefault("host") is { Length: > 0 } names)
        {
            foreach (string name in names.Split(',').Select(host => host.Trim()))
            {
                hosts.Add((name, name.Length == 0 || name[0] is '/' or '@' || IPAddress.TryParse(name, out _) ? null : Task.Run(() => Dns.GetHostAddresses(name))));
            }

            ports = options.GetValueOrDefault("port") is { Length: > 0 } port ? [.. port.Split(',').Select(each => each.Trim())] : [];
        }

        WorfProbe probe = new(conninfo, applicationName, peer, Worf.ToServerCString(setup), deadline, hosts, ports);
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
                    if (!Ready(ConnectFlag(), 0))
                    {
                        return;
                    }

                    _polling = ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQconnectPoll"))(_conn);
                    continue;
                }

                if (_polling == PollingFailed || Status() != ConnectionOk)
                {
                    Failure = ErrorMessage();
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
        }
        else
        {
            Failure ??= "the connection has no socket";
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

    // Starts libpq's connection, to the addresses the host names' lookups found, which a connection that libpq finds bad
    // at once, as for an option it doesn't know, fails.
    private void Connect()
    {
        // Each address a name has goes as a host of its own, under the name, with the name's port, so that libpq tries
        // them in turn, as it would the addresses it looked up itself.
        List<string> names = [];
        List<string> addresses = [];
        List<string> ports = [];
        for (int i = 0; i < _hosts.Count; i++)
        {
            (string host, Task<IPAddress[]>? lookup) = _hosts[i];
            string port = _ports.Length == 1 ? _ports[0] : i < _ports.Length ? _ports[i] : "";
            if (lookup is null)
            {
                names.Add(host);
                addresses.Add(IPAddress.TryParse(host, out _) ? host : "");
                ports.Add(port);
            }
            else if (lookup.IsCompletedSuccessfully && lookup.Result.Length > 0)
            {
                foreach (IPAddress address in lookup.Result)
                {
                    names.Add(host);
                    addresses.Add(address.ToString());
                    ports.Add(port);
                }
            }
            else
            {
                Failure = $"could not translate host name \"{host}\" to address: {(lookup.Exception?.InnerException as SocketException)?.Message ?? "no address found"}";
                return;
            }
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
        var start = (delegate* unmanaged<sbyte**, sbyte**, int, void*>)WorfConnection.LibpqFunction("PQconnectStartParams");
        byte[] info = Worf.ToServerCString(_conninfo);
        byte[] application = Worf.ToServerCString(_applicationName);
        byte[] hostValue = Worf.ToServerCString(string.Join(',', names));
        byte[] hostaddr = Worf.ToServerCString(string.Join(',', addresses));
        byte[] portValue = Worf.ToServerCString(string.Join(',', ports));
        bool listed = names.Count > 0;
        bool ported = listed && _ports.Length > 0;
        fixed (byte* dbname = "dbname\0"u8, fallback = "fallback_application_name\0"u8, hostKey = "host\0"u8, hostaddrKey = "hostaddr\0"u8,
            portKey = "port\0"u8, infoAddress = info, applicationAddress = application, hostAddress = hostValue, hostaddrValue = hostaddr,
            portAddress = portValue)
        {
            // The hosts, their addresses and ports come after the connection string, whose values they take the place of.
            sbyte** keys = stackalloc sbyte*[]
            {
                (sbyte*)dbname, (sbyte*)fallback, listed ? (sbyte*)hostKey : null, listed ? (sbyte*)hostaddrKey : null, ported ? (sbyte*)portKey : null, null,
            };
            sbyte** values = stackalloc sbyte*[]
            {
                (sbyte*)infoAddress, (sbyte*)applicationAddress, (sbyte*)hostAddress, (sbyte*)hostaddrValue, (sbyte*)portAddress, null,
            };
            _conn = start(keys, values, 1);
        }

        if (_conn is null)
        {
            Failure = "out of memory";
            return;
        }

        WorfNotices.Route(_conn, _peer);
        if (Status() == ConnectionBad)
        {
            Failure = ErrorMessage();
        }
    }

    // The options libpq would connect with, from the connection string and, for those it doesn't give, a service file or
    // the environment, or null: libpq takes them all in as it starts a connection, before it looks a host up, and stops
    // there at the session setting, which no server takes.
    private static Dictionary<string, string>? EffectiveOptions(string conninfo, string applicationName)
    {
        var start = (delegate* unmanaged<sbyte**, sbyte**, int, void*>)WorfConnection.LibpqFunction("PQconnectStartParams");
        var finish = (delegate* unmanaged<void*, void>)WorfConnection.LibpqFunction("PQfinish");
        byte[] info = Worf.ToServerCString(conninfo);
        byte[] application = Worf.ToServerCString(applicationName);
        void* conn;
        fixed (byte* dbname = "dbname\0"u8, fallback = "fallback_application_name\0"u8, attrs = "target_session_attrs\0"u8, nothing = "worf-options\0"u8,
            infoAddress = info, applicationAddress = application)
        {
            sbyte** keys = stackalloc sbyte*[] { (sbyte*)dbname, (sbyte*)fallback, (sbyte*)attrs, null };
            sbyte** values = stackalloc sbyte*[] { (sbyte*)infoAddress, (sbyte*)applicationAddress, (sbyte*)nothing, null };
            conn = start(keys, values, 1);
        }

        if (conn is null)
        {
            return null;
        }

        try
        {
            byte* options = (byte*)((delegate* unmanaged<void*, void*>)WorfConnection.LibpqFunction("PQconninfo"))(conn);
            if (options is null)
            {
                return null;
            }

            // PQconninfoOption: its keyword, environment variable, compiled default, value, label and display character,
            // and its display size, to a keyword of null.
            int stride = ((6 * sizeof(nint)) + sizeof(int) + sizeof(nint) - 1) / sizeof(nint) * sizeof(nint);
            Dictionary<string, string> found = new(StringComparer.Ordinal);
            try
            {
                for (byte* option = options; *(sbyte**)option is not null; option += stride)
                {
                    sbyte* value = *(sbyte**)(option + (3 * sizeof(nint)));
                    if (value is not null)
                    {
                        found[Worf.FromServer(*(sbyte**)option)] = Worf.FromServer(value);
                    }
                }
            }
            finally
            {
                ((delegate* unmanaged<void*, void>)WorfConnection.LibpqFunction("PQconninfoFree"))(options);
            }

            return found;
        }
        finally
        {
            finish(conn);
        }
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
