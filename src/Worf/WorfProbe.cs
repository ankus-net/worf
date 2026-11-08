using Ankus.Postgres;
#if WORF_WINDOWS
using PgSocket = ulong;
#else
using PgSocket = int;
#endif

namespace Worf;

/// <summary>
/// A connection to a neighbor that the exchange worker makes without waiting for it, as for a neighbor whose last call
/// ran out of time, so that a neighbor that never answers holds up none of the worker's calls to the others: libpq
/// connects, and runs the session's setup, as the worker looks at its socket between its calls, and the connection,
/// once the neighbor has answered, becomes the one the worker keeps for its next call. libpq's polling, status and
/// result values are part of its stable interface.
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

    // PGRES_POLLING_FAILED, READING, WRITING and OK, CONNECTION_OK and CONNECTION_STARTED, and PGRES_COMMAND_OK and
    // PGRES_TUPLES_OK.
    private const int PollingFailed = 0;
    private const int PollingReading = 1;
    private const int PollingWriting = 2;
    private const int ConnectionOk = 0;
    private const int ConnectionStarted = 2;
    private const int CommandOk = 1;
    private const int TuplesOk = 2;

    private readonly byte[] _setup;
    private void* _conn;
    private int _polling = PollingWriting;
    private bool _sent;
    private bool _fd;

    private WorfProbe(void* conn, byte[] setup, long deadline, bool fd)
    {
        _conn = conn;
        _setup = setup;
        _fd = fd;
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
    /// Starts connecting.
    /// </summary>
    /// <param name="conninfo">The connection string.</param>
    /// <param name="applicationName">The name the server shows for the connection.</param>
    /// <param name="setup">The statements the session runs once connected, in one round trip.</param>
    /// <param name="deadline">When the probe runs out of time, as a timestamp.</param>
    /// <returns>The probe.</returns>
    internal static WorfProbe Start(string conninfo, string applicationName, string setup, long deadline)
    {
        // From PostgreSQL 18, libpqwalreceiver counts the connections it closes against the server's external files, as
        // it counts those it opens.
        bool fd = false;
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
        if (!NativeMethods.AcquireExternalFD())
        {
            throw new PgException(PgSqlStates.ConnectionFailure, "could not connect to the server: too many open files");
        }

        fd = true;
#endif
        var start = (delegate* unmanaged<sbyte**, sbyte**, int, void*>)WorfConnection.LibpqFunction("PQconnectStartParams");
        byte[] info = Worf.ToServerCString(conninfo);
        byte[] application = Worf.ToServerCString(applicationName);
        void* conn;
        fixed (byte* dbname = "dbname\0"u8, fallback = "fallback_application_name\0"u8, infoAddress = info, applicationAddress = application)
        {
            sbyte** keys = stackalloc sbyte*[] { (sbyte*)dbname, (sbyte*)fallback, null };
            sbyte** values = stackalloc sbyte*[] { (sbyte*)infoAddress, (sbyte*)applicationAddress, null };
            conn = start(keys, values, 1);
        }

        if (conn is null)
        {
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
            NativeMethods.ReleaseExternalFD();
#endif
            throw new PgException(PgSqlStates.OutOfMemory, "out of memory");
        }

        WorfProbe probe = new(conn, Worf.ToServerCString(setup), deadline, fd);
        probe.Advance();
        return probe;
    }

    /// <summary>
    /// Takes the probe as far as its socket lets it now, without waiting: connects, sends the setup, and reads its
    /// results.
    /// </summary>
    internal void Advance()
    {
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

                if (_polling == PollingFailed || ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQstatus"))(_conn) != ConnectionOk)
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
    /// Waits up to a time for the probe's socket to be ready for its next step, or for the latch.
    /// </summary>
    /// <param name="milliseconds">The time.</param>
    /// <returns>Whether the latch was set, which the wait resets.</returns>
    internal bool Wait(long milliseconds)
    {
        int flag = _sent ? WlSocketReadable : ConnectFlag();
        int events = NativeMethods.WaitLatchOrSocket(NativeGlobals.MyLatch, WlExitOnPmDeath | WlLatchSet | WlTimeout | flag, Socket(),
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
        _fd = false;
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

        ((delegate* unmanaged<void*, void>)WorfConnection.LibpqFunction("PQfinish"))(_conn);
        _conn = null;
#if !(ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17)
        if (_fd)
        {
            NativeMethods.ReleaseExternalFD();
        }
#endif
        _fd = false;
    }

    // What the connection waits for: to read or to write, or on Windows, while the connect itself is under way, for it
    // to finish, as Windows reports a refused connect as writable, which would fail it rather than try the next address.
    private int ConnectFlag()
        => _polling == PollingReading ? WlSocketReadable
            : OperatingSystem.IsWindows() && ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQstatus"))(_conn) == ConnectionStarted
                ? WlSocketConnected : WlSocketWriteable;

    // Whether the socket is ready for what the probe waits for, within a time.
    private bool Ready(int flag, long milliseconds)
        => (NativeMethods.WaitLatchOrSocket(null, WlExitOnPmDeath | WlTimeout | flag, Socket(), milliseconds, PgWaitExtension) & flag) != 0;

    private PgSocket Socket() => WorfConnection.ToSocket(((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQsocket"))(_conn));

    private string ErrorMessage()
        => Worf.FromServer(((delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction("PQerrorMessage"))(_conn)).TrimEnd();
}
