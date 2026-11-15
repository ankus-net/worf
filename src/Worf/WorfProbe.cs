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
/// A connection that a worf process makes without blocking on it: the exchange worker's to a neighbor, between its other
/// work, so that a neighbor that never answers, or answers slowly, holds up none of the worker's calls to the others, and
/// an apply or sync worker's, which it waits for. The host names libpq would connect to, from the connection string, a
/// service file or the environment, are looked up on other threads, and libpq connects to each address as its name's
/// lookup finds it, in turn, as libpq does, giving each the connection's <c>connect_timeout</c>, and passing over a name
/// that has none; then the session runs its setup. libpq is given one address at a time, followed by a host that
/// can't be connected to, so that where it stops tells whether it would have gone on past the address, and to what. A logical replication connection asks for this database's encoding
/// and settings that make the values it gets unambiguous, after any options the connection string gives, as
/// libpqwalreceiver's do. libpq's polling, status and result values are part of its stable interface.
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

    // A host after the address libpq is given that it can't connect to, as a socket directory that doesn't exist, so that
    // libpq failing there shows it went past the address, as it does past one that doesn't take the connection or a
    // server that isn't of the session's attributes, rather than stopped at a server that refused it.
    private const string EndOfList = "/worf-probe-end-of-list";

    private readonly string _conninfo;
    private readonly string _applicationName;
    private readonly string _peer;
    private readonly uint _peerNode;
    private readonly byte[] _setup;
    private readonly List<(string Host, Task<IPAddress[]>? Lookup)> _hosts;
    private readonly bool[] _harvested;
    private readonly int[] _order;
    private readonly string[] _ports;
    private readonly long _connectTimeout;
    private readonly bool _random;
    private readonly string? _options;
    private readonly (uint Connect, uint Receive) _waits;
    private readonly List<string> _failures = [];
    private readonly List<(string Host, string Address, string Port, int Entry)> _attempts = [];
    private readonly bool _preferStandby;
    private int _attempt = -1;
    private long _attemptDeadline;
    private void* _conn;
    private int _polling = PollingWriting;
    private bool _sent;
    private bool _reached;
    private bool _standbyOnly;
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
    private bool _fd;
#endif

    private WorfProbe(string conninfo, string applicationName, (string Name, uint Node) peer, byte[] setup, long deadline, List<(string Host, Task<IPAddress[]>? Lookup)> hosts,
        string[] ports, long connectTimeout, bool random, bool preferStandby, string? options, (uint Connect, uint Receive) waits)
    {
        _conninfo = conninfo;
        _applicationName = applicationName;
        (_peer, _peerNode) = peer;
        _setup = setup;
        _hosts = hosts;
        _harvested = new bool[hosts.Count];
        _order = [.. Enumerable.Range(0, hosts.Count)];
        if (random)
        {
            Random.Shared.Shuffle(_order);
        }

        _ports = ports;
        _connectTimeout = connectTimeout;
        _random = random;
        _preferStandby = preferStandby;
        _standbyOnly = preferStandby;
        _options = options;
        _waits = waits;
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
    /// <param name="setup">The statements the session runs once connected, in one round trip, or none.</param>
    /// <param name="deadline">When the probe runs out of time, as a timestamp, or <see cref="long.MaxValue"/> for never.</param>
    /// <param name="replication">Whether to connect for logical replication.</param>
    /// <param name="waits">The wait events to wait on the connection's socket with, while connecting and while its setup
    /// runs, or null for worf's own.</param>
    /// <returns>The probe.</returns>
    internal static WorfProbe Start(string conninfo, string applicationName, (string Name, uint Node) peer, string setup, long deadline,
        bool replication = false, (uint Connect, uint Receive)? waits = null)
    {
        // A host given as an address, or a socket's directory, needs no lookup; libpq looks up the others as it connects,
        // which would hold the worker, those a service file or the environment gives too, and a name whose hostaddr is
        // empty. Addresses the connection gives stand for its hosts, as libpq takes them, and so do lists that libpq
        // refuses for counts that don't match.
        List<(string Host, Task<IPAddress[]>? Lookup)> hosts = [];
        string[] ports = [];
        string? refused = null;
        Dictionary<string, string>? options = WorfConnection.EffectiveOptions(conninfo, applicationName);
        if (options is not null)
        {
            // Each entry is taken as it is, spaces and all, as libpq takes it.
            string[] names = options.GetValueOrDefault("host") is { Length: > 0 } host ? host.Split(',') : [];
            string[] addresses = options.GetValueOrDefault("hostaddr") is { Length: > 0 } given ? given.Split(',') : [];
            ports = options.GetValueOrDefault("port") is { Length: > 0 } port ? [.. port.Split(',').Select(each => each.Trim())] : [];
            int count = Math.Max(addresses.Length > 0 ? addresses.Length : names.Length, 1);
            if (names.Length > 0 && addresses.Length > 0 && names.Length != addresses.Length)
            {
                refused = $"could not match {names.Length.ToString(CultureInfo.InvariantCulture)} host names to {addresses.Length.ToString(CultureInfo.InvariantCulture)} hostaddr values";
            }
            else if (ports.Length > 1 && ports.Length != count)
            {
                refused = $"could not match {ports.Length.ToString(CultureInfo.InvariantCulture)} port numbers to {count.ToString(CultureInfo.InvariantCulture)} hosts";
            }
            else
            {
                for (int i = 0; i < Math.Max(names.Length, addresses.Length); i++)
                {
                    string name = i < names.Length ? names[i] : "";
                    string address = i < addresses.Length ? addresses[i] : "";
                    hosts.Add(address.Length > 0 ? (name + "\0" + address, null)
                        : (name, name.Length == 0 || name[0] is '/' or '@' || (name.Trim() == name && IPAddress.TryParse(name, out _)) ? null : HostLookup.Start(name)));
                }
            }
        }

        // libpq waits at least 2 seconds for each address when connect_timeout sets a time, before PostgreSQL 17.
        long connectTimeout = options?.GetValueOrDefault("connect_timeout") is string seconds &&
            int.TryParse(seconds, NumberStyles.Integer, CultureInfo.InvariantCulture, out int timeout) && timeout > 0 ?
#if ANKUS_PG15 || ANKUS_PG16
            Math.Max(timeout, 2) * 1_000_000L : 0;
#else
            timeout * 1_000_000L : 0;
#endif
        // A logical replication connection's own options come after those the connection string gives, as libpq keeps
        // only the last value of each keyword.
        string? replicationOptions = replication
            ? $"{options?.GetValueOrDefault("options")} -c datestyle=ISO -c intervalstyle=postgres -c extra_float_digits=3".TrimStart()
            : null;
        WorfProbe probe = new(conninfo, applicationName, peer, setup.Length > 0 ? Worf.ToServerCString(setup) : [], deadline, hosts, ports, connectTimeout,
            options?.GetValueOrDefault("load_balance_hosts") == "random", options?.GetValueOrDefault("target_session_attrs") == "prefer-standby",
            replicationOptions, waits ?? (PgWaitExtension, PgWaitExtension));
        if (refused is not null)
        {
            probe.Failure = refused;
            return probe;
        }

        probe.Advance();
        return probe;
    }

    /// <summary>
    /// Takes the probe as far as it goes now, without waiting: looks for the host names' addresses, connects, sends the
    /// setup, and reads its results.
    /// </summary>
    internal void Advance()
    {
        if (Done)
        {
            return;
        }

        if (Harvest() && _conn is not null && !_sent && _attemptDeadline != 0)
        {
            // Addresses found since the one being connected to started share the time left with it.
            _attemptDeadline = Math.Min(_attemptDeadline, AttemptDeadline());
        }

        if (_conn is null)
        {
            Next();
        }

        while (_conn is not null && !Done)
        {
            if (!_sent)
            {
                if (_polling is PollingReading or PollingWriting)
                {
                    // An address that doesn't connect within its time gives way to the next, as libpq's own wait for a
                    // connection does with connect_timeout.
                    NoteReached();
                    if (_attemptDeadline != 0 && Worker.GetCurrentTimestamp() >= _attemptDeadline)
                    {
                        TimedOut();
                        continue;
                    }

                    if (Socket() == NoSocket)
                    {
                        if (GoesOn())
                        {
                            Next();
                            continue;
                        }

                        return;
                    }

                    if (!Ready(ConnectFlag(), 0))
                    {
                        return;
                    }

                    _polling = ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQconnectPoll"))(_conn);
                    continue;
                }

                // libpq has failed at the address, and gone on past it, as it would to the next, or stopped at a server
                // that refused the connection, as it does.
                if (_polling == PollingFailed || Status() != ConnectionOk)
                {
                    if (GoesOn())
                    {
                        Next();
                        continue;
                    }

                    return;
                }

                // A connection with no setup to run is answered once it's made.
                if (_setup.Length == 0)
                {
                    Answered = true;
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

            // Windows records a connect's end once, for the wait set that asked at the time, so one that ends between
            // waits is seen as the probe next looks, a moment later.
            if (flag == WlSocketConnected)
            {
                milliseconds = Math.Min(milliseconds, 20);
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
            Math.Max(milliseconds, 0), _sent ? _waits.Receive : _waits.Connect);
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
    internal WorfConnection Take() => WorfConnection.FromLibpq(TakeLibpq());

    /// <summary>
    /// Takes the libpq connection the probe made, once the server answered, with its count against the server's
    /// external files.
    /// </summary>
    /// <returns>The libpq connection.</returns>
    internal void* TakeLibpq()
    {
        void* conn = _conn;
        _conn = null;
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
        _fd = false;
#endif
        return conn;
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

    // Lists the addresses found so far, each with its name and port, in the order of the names, as libpq tries them, up
    // to the first name whose lookup hasn't finished, passing over a name that has none, in random order when the
    // connection asks libpq to balance its hosts. Returns whether it listed any.
    private bool Harvest()
    {
        if (_hosts.Count == 0 && _attempts.Count == 0)
        {
            // A connection string that names no host connects as libpq's defaults have it.
            _attempts.Add(("", "", "", 0));
            return true;
        }

        int listed = _attempts.Count;
        foreach (int index in _order)
        {
            (string host, Task<IPAddress[]>? lookup) = _hosts[index];
            if (_harvested[index])
            {
                continue;
            }

            if (lookup is { IsCompleted: false })
            {
                break;
            }

            _harvested[index] = true;
            string port = _ports.Length == 1 ? _ports[0] : index < _ports.Length ? _ports[index] : "";
            if (lookup is null)
            {
                // A host given with its address, as hostaddr gives it, is the name and the address apart.
                int split = host.IndexOf('\0', StringComparison.Ordinal);
                _attempts.Add(split >= 0 ? (host[..split], host[(split + 1)..], port, index) : (host, IPAddress.TryParse(host, out _) ? host : "", port, index));
            }
            else if (lookup.IsCompletedSuccessfully && lookup.Result.Length > 0)
            {
                HostLookup.Took(host, lookup);
                IPAddress[] found = [.. lookup.Result];
                if (_random)
                {
                    Random.Shared.Shuffle(found);
                }

                _attempts.AddRange(found.Select(address => (host, address.ToString(), port, index)));
            }
            else
            {
                HostLookup.Took(host, lookup);
                _failures.Add($"could not translate host name \"{host}\" to address: {lookup.Exception?.InnerException?.Message ?? "no address found"}");
            }
        }

        return _attempts.Count > listed;
    }

    // The names not listed yet, as their lookups, or an earlier name's, haven't finished, which count as an address each.
    private int Unlisted() => Array.FindAll(_harvested, harvested => !harvested).Length;

    // Connects to the next address found, which libpq tries, and goes on past as it would: to the name's next address
    // after one that doesn't take the connection, and to the next name's after a server that is starting or isn't of the
    // session's attributes, but not after one that refuses the connection, as for a role it doesn't have. A connection
    // that libpq finds bad at once, as for an option it doesn't know, fails. With no address left while names are still
    // looked up, it waits for them.
    private void Next()
    {
        // A connection that prefers a standby makes libpq's two passes itself: the first takes a standby only, through
        // every address, a name's found later too, and the second, once every name is found, takes any server, from the
        // first host again, as libpq's own second pass starts there. With no address found at all, there is no second
        // pass to make, as libpq has none.
        // Names are still to come until each lookup's outcome is listed, which a lookup that ends now waits for, rather
        // than the probe fail without it.
        if (_attempt + 1 >= _attempts.Count)
        {
            if (_standbyOnly && Unlisted() == 0 && _attempts.Count > 0)
            {
                _standbyOnly = false;
                _attempt = -1;
            }
            else
            {
                if (Unlisted() == 0)
                {
                    Failure = Failures(null);
                }

                return;
            }
        }

        // From PostgreSQL 18, libpqwalreceiver counts the connections it closes against the server's external files, as
        // it counts those it opens.
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
        if (!_fd)
        {
            if (!NativeMethods.AcquireExternalFD())
            {
                Failure = "could not connect to the server: too many open files";
                return;
            }

            _fd = true;
        }
#endif
        _attempt++;
        var start = (delegate* unmanaged<sbyte**, sbyte**, int, void*>)WorfConnection.LibpqFunction("PQconnectStartParams");
        List<(string Host, string Address, string Port, int Entry)> rest = [_attempts[_attempt], (EndOfList, "", _attempts[_attempt].Port, -1)];
        byte[] info = Worf.ToServerCString(_conninfo);
        byte[] application = Worf.ToServerCString(_applicationName);

        byte[] hostValue = Worf.ToServerCString(string.Join(',', rest.Select(attempt => attempt.Host)));
        byte[] hostaddr = Worf.ToServerCString(string.Join(',', rest.Select(attempt => attempt.Address)));
        byte[] portValue = Worf.ToServerCString(string.Join(',', rest.Select(attempt => attempt.Port)));
        byte[] encoding = Worf.ToServerCString(Worf.DatabaseEncodingName);
        byte[] options = Worf.ToServerCString(_options ?? "");
        bool listed = _hosts.Count > 0;
        bool ported = listed && _ports.Length > 0;
        bool replication = _options is not null;
        fixed (byte* dbname = "dbname\0"u8, fallback = "fallback_application_name\0"u8, hostKey = "host\0"u8, hostaddrKey = "hostaddr\0"u8,
            portKey = "port\0"u8, balanceKey = "load_balance_hosts\0"u8, balanceValue = "disable\0"u8, replicationKey = "replication\0"u8,
            attributesKey = "target_session_attrs\0"u8, standby = "standby\0"u8, any = "any\0"u8,
            logical = "database\0"u8, encodingKey = "client_encoding\0"u8, optionsKey = "options\0"u8, infoAddress = info,
            applicationAddress = application, hostAddress = hostValue, hostaddrValue = hostaddr, portAddress = portValue, encodingValue = encoding,
            optionsValue = options)
        {
            // The address, its host and port come after the connection string, whose values they take the place of,
            // and so do a replication connection's own options.
            List<(nint Key, nint Value)> pairs = [((nint)dbname, (nint)infoAddress), ((nint)fallback, (nint)applicationAddress)];
            if (listed)
            {
                pairs.Add(((nint)hostKey, (nint)hostAddress));
                pairs.Add(((nint)hostaddrKey, (nint)hostaddrValue));
            }

            if (ported)
            {
                pairs.Add(((nint)portKey, (nint)portAddress));
            }

            if (_random)
            {
                pairs.Add(((nint)balanceKey, (nint)balanceValue));
            }

            if (_preferStandby)
            {
                pairs.Add(((nint)attributesKey, _standbyOnly ? (nint)standby : (nint)any));
            }

            if (replication)
            {
                pairs.Add(((nint)replicationKey, (nint)logical));
                pairs.Add(((nint)encodingKey, (nint)encodingValue));
                pairs.Add(((nint)optionsKey, (nint)optionsValue));
            }

            sbyte** keys = stackalloc sbyte*[pairs.Count + 1];
            sbyte** values = stackalloc sbyte*[pairs.Count + 1];
            for (int i = 0; i < pairs.Count; i++)
            {
                keys[i] = (sbyte*)pairs[i].Key;
                values[i] = (sbyte*)pairs[i].Value;
            }

            keys[pairs.Count] = null;
            values[pairs.Count] = null;
            _conn = start(keys, values, 1);
        }

        if (_conn is null)
        {
            Failure = "out of memory";
            return;
        }

        Route();
        _polling = PollingWriting;
        _reached = false;
        StartAttempt();
        if (Status() == ConnectionBad && GoesOn())
        {
            Next();
        }
    }

    // libpq failed, as soon as it started too, as at an address that refuses at once. Gone on past the address, it goes
    // to the next, or, past a server it reached, to the next name's first, as libpq moves past such a server to the next
    // host; stopped at a server that refused it, the probe fails, as libpq does. Returns whether it goes on.
    private bool GoesOn()
    {
        string message = ErrorMessage();
        int end = message.IndexOf(EndOfList, StringComparison.Ordinal);
        if (end >= 0)
        {
            message = message[..(message.LastIndexOf('\n', end) + 1)].TrimEnd();
        }

        message = Named(message);
        if (Text("PQhost") != EndOfList)
        {
            Failure = Failures(message.Length > 0 ? message : null);
            return false;
        }

        if (message.Length > 0)
        {
            _failures.Add(message);
        }

        Close();
        if (_reached)
        {
            int entry = _attempts[_attempt].Entry;
            _attempt = _attempts.FindLastIndex(attempt => attempt.Entry == entry);
        }

        return true;
    }

    // Closes the connection being made, to start libpq again.
    private void Close()
    {
        WorfNotices.Forget(_conn);
        ((delegate* unmanaged<void*, void>)WorfConnection.LibpqFunction("PQfinish"))(_conn);
        _conn = null;
    }

    // Gives an address its time.
    private void StartAttempt() => _attemptDeadline = AttemptDeadline();

    // When the address being connected to gives way: at the connection's connect_timeout, or with none, or a longer one,
    // at its share of what the probe has left, divided among the addresses not yet tried, a name not yet listed counting
    // as one, so that one that never answers leaves the others time; or never, for the last with no connect_timeout.
    private long AttemptDeadline()
    {
        long now = Worker.GetCurrentTimestamp();
        int left = Math.Max(_attempts.Count - _attempt + Unlisted(), 1);
        long share = Math.Max((Deadline - now) / left, 0);
        return _connectTimeout <= 0 && left == 1 ? 0
            : now + (_connectTimeout > 0 ? Math.Min(_connectTimeout, left > 1 ? share : _connectTimeout) : share);
    }

    // Names the server whose notices the connection gets: the node, or the host and port libpq connects to now.
    private void Route()
    {
        if (_peerNode != 0)
        {
            WorfNotices.Route(_conn, _peer, _peerNode);
        }
        else
        {
            WorfNotices.RouteByAddress(_conn);
        }
    }

    // Notes whether the server at the address answered: CONNECTION_AWAITING_RESPONSE, AUTH_OK, SETENV, CHECK_WRITABLE,
    // CONSUME, CHECK_TARGET and CHECK_STANDBY.
    private void NoteReached() => _reached |= Status() is 4 or 5 or 6 or 9 or 10 or 12 or 13;

    // libpq's message for an address a name's lookup found, which libpq names alone, as it was given it, naming the name
    // with it, as libpq does as it looks the name up itself.
    private string Named(string message)
    {
        (string host, string address, _, int entry) = _attempt >= 0 && _attempt < _attempts.Count ? _attempts[_attempt] : ("", "", "", -1);
        return LookedUp(entry) && address.Length > 0 && host != address
            ? string.Join('\n', message.Split('\n').Select(line => NamedLine(line, host, address)))
            : message;
    }

    // A line with the first address in it that is quoted, as libpq names one, named by its name as well.
    private static string NamedLine(string line, string host, string address)
    {
        static bool Quote(char c) => !char.IsLetterOrDigit(c) && !char.IsWhiteSpace(c) && c is not ('.' or ':' or '%' or '_' or '-');
        for (int at = line.IndexOf(address, StringComparison.Ordinal); at >= 0; at = line.IndexOf(address, at + 1, StringComparison.Ordinal))
        {
            int end = at + address.Length;
            if (at > 0 && end < line.Length && Quote(line[at - 1]) && Quote(line[end]))
            {
                return string.Concat(line[..at], host, line[end..(end + 1)], " (", address, ")", line[(end + 1)..]);
            }
        }

        return line;
    }

    // Whether a host entry's addresses came from its name's lookup.
    private bool LookedUp(int entry) => entry >= 0 && entry < _hosts.Count && _hosts[entry].Lookup is not null;

    // A connection's host, address or port, as libpq gives it now.
    private string Text(string function)
    {
        sbyte* value = ((delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction(function))(_conn);
        return value is null ? "" : Worf.FromServer(value);
    }

    // Gives up the address being connected to at its time, and connects to those after it.
    private void TimedOut()
    {
        _failures.Add(AttemptFailure("timeout expired"));
        Close();
        Next();
    }

    // The probe's failure: the names it found no address for, the addresses that ran out of time, and libpq's own.
    private string Failures(string? last)
        => string.Join('\n', last is null ? _failures : [.. _failures, last.TrimEnd()]) is { Length: > 0 } all ? all : "could not connect to the server";

    // A failure of the address being connected to, worded as libpq words its own: an address it was given alone, and one
    // it found with its name.
    private string AttemptFailure(string reason)
    {
        (string host, string address, string port, int entry) = _attempt >= 0 && _attempt < _attempts.Count ? _attempts[_attempt] : ("", "", "", -1);
        string at = LookedUp(entry) && address.Length > 0 && address != host ? $"\"{host}\" ({address})" : $"\"{(address.Length > 0 ? address : host)}\"";
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
            Failure ??= ErrorMessage() is { Length: > 0 } message ? Named(message) : "the connection has no socket";
            return false;
        }

        if (flag == WlSocketConnected)
        {
            return ConnectEnded(socket);
        }

        return (NativeMethods.WaitLatchOrSocket(null, WlExitOnPmDeath | WlTimeout | flag, socket, milliseconds, _sent ? _waits.Receive : _waits.Connect) & flag) != 0;
    }

    // Whether a connect under way has ended, made or failed, as select() shows it each time it's asked: Windows records
    // the end of a connect in a wait set's event once, for the set that asked for it then, and every wait makes a set of
    // its own, so a connect refused between waits would otherwise never be seen to end.
    private static bool ConnectEnded(PgSocket socket)
    {
#if WORF_WINDOWS
        using SafeSocketHandle handle = new(unchecked((nint)socket), ownsHandle: false);
#else
        using SafeSocketHandle handle = new(socket, ownsHandle: false);
#endif
        using Socket connecting = new(handle);
        return connecting.Poll(0, SelectMode.SelectWrite) || connecting.Poll(0, SelectMode.SelectError);
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
