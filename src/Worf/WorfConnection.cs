using System.Runtime.InteropServices;
using Ankus.Postgres;
#if WORF_WINDOWS
using PgSocket = ulong;
#else
using PgSocket = int;
#endif

namespace Worf;

/// <summary>
/// A connection to another node, for replication or for SQL and copies, through libpqwalreceiver, as PostgreSQL's own
/// subscriber connects. Its text is the database encoding's, byte for byte, as the connection's client encoding is:
/// a name a SQL_ASCII database holds that isn't valid UTF-8 travels as its bytes, which <see cref="Worf.ToServer"/>
/// and <see cref="Worf.FromServer(ReadOnlySpan{byte})"/> keep.
/// </summary>
internal sealed unsafe partial class WorfConnection : IDisposable
{
    private const int WlLatchSet = 1;
    private const int WlSocketReadable = 2;
    private const int WlSocketWriteable = 4;
    private const int WlTimeout = 8;
    private const int WlExitOnPmDeath = 32;
    private const int WlSocketConnected = 64;
    private const uint PgWaitExtension = 0x07000000;

    // WAIT_EVENT_LIBPQWALRECEIVER_CONNECT and WAIT_EVENT_LIBPQWALRECEIVER_RECEIVE, the client wait events libpqwalreceiver
    // shows as it connects and as it waits for a result, the fourth and fifth of PG_WAIT_CLIENT's on every version.
    private const uint WaitLibpqConnect = 0x06000003;
    private const uint WaitLibpqReceive = 0x06000004;

    // The copy being read: COPY's data source takes no argument, and a backend runs one copy at a time.
    private static WorfConnection? s_copySource;
    private static byte* s_copyPending;
    private static int s_copyPendingLength;
    private static bool s_copyEnded;

    private readonly int _thread = Environment.CurrentManagedThreadId;
    private WalReceiverConn* _conn;
    private readonly MemoryContextData* _scratch;
    private byte[] _message = new byte[8192];

    // The messages of a copy read ahead of their turn, as while apply waits out its delay, which Receive gives before it
    // reads more, how many bytes they hold, and whether the copy ended after them.
    private readonly Queue<byte[]> _readAhead = new();
    private long _readAheadBytes;
    private bool _endedAhead;

    private WorfConnection(WalReceiverConn* conn, MemoryContextData* scratch, int serverVersion)
    {
        _conn = conn;
        _scratch = scratch;
        ServerVersion = serverVersion;
    }

    /// <summary>
    /// Gets the server's version number, such as 180006 for PostgreSQL 18.6.
    /// </summary>
    internal int ServerVersion { get; }

    /// <summary>
    /// Gets the libpq connection of an ordinary connection, which libpqwalreceiver's WalReceiverConn holds first, in the
    /// layout it has kept since PostgreSQL 10, for a query sent without waiting on it.
    /// </summary>
    internal void* Libpq => ((OrdinaryWalReceiverConn*)Conn)->StreamConn;

    /// <summary>
    /// Gets what an ordinary connection sets before it runs anything else, which a logical replication connection asks
    /// for in its startup packet: text in this database's encoding, unambiguous dates, intervals and floats, and an
    /// always-secure search path.
    /// </summary>
    internal static string OrdinarySettings => "SET search_path = ''; SET client_encoding = '" + Worf.DatabaseEncodingName +
        "'; SET datestyle = 'ISO'; SET intervalstyle = 'postgres'; SET extra_float_digits = 3";

    /// <summary>
    /// Gets when the last message of a streaming copy arrived, or when the copy started, as a timestamp.
    /// </summary>
    internal long LastReceivedAt { get; private set; }

    [PgNativeCallback(nameof(ReadCopy))]
    private static partial copy_data_source_cb CopySource { get; }

    private static WalReceiverFunctionsType* Functions
    {
        get
        {
            if (NativeGlobals.WalReceiverFunctions is null)
            {
                fixed (byte* library = "libpqwalreceiver\0"u8)
                {
                    NativeMethods.load_file((sbyte*)library, false);
                }
            }

            return NativeGlobals.WalReceiverFunctions;
        }
    }

    private WalReceiverConn* Conn
    {
        get
        {
            ObjectDisposedException.ThrowIf(_conn is null, this);
            if (Environment.CurrentManagedThreadId != _thread)
            {
                throw new InvalidOperationException("A connection belongs to the backend thread that opened it.");
            }

            return _conn;
        }
    }

    /// <summary>
    /// Opens a logical replication connection, or an ordinary connection for SQL and copies.
    /// </summary>
    /// <param name="conninfo">A libpq connection string naming the server and database.</param>
    /// <param name="applicationName">The name the server shows for the connection.</param>
    /// <param name="replication">Whether to connect for logical replication, rather than for SQL and copies.</param>
    /// <param name="node">The node the server is, whose name the log gives its notices and against which they count, or
    /// none.</param>
    /// <returns>The open connection.</returns>
    internal static WorfConnection Connect(string conninfo, string applicationName, bool replication, (string Name, uint Id) node = default)
    {
        byte[] info = Worf.ToServerCString(conninfo);
        byte[] application = Worf.ToServerCString(applicationName);
        WalReceiverFunctionsType* functions = Functions;
        WalReceiverConn* conn;
        sbyte* failure = null;

        // The connection lives until it's closed.
        MemoryContextData* caller = Worf.MemoryContextSwitchTo(NativeGlobals.TopMemoryContext);
        try
        {
            fixed (byte* infoAddress = info, applicationAddress = application)
            {
                conn = ConnectLibpq((sbyte*)infoAddress, (sbyte*)applicationAddress, replication, node, &failure);
            }
        }
        finally
        {
            _ = Worf.MemoryContextSwitchTo(caller);
        }

        if (conn is null)
        {
            string message = failure is null ? "unknown error" : Worf.FromServer(failure);
            throw new PgException(PgSqlStates.ConnectionFailure,
                (replication ? "could not connect for replication: " : "could not connect to the server: ") + message);
        }

        if (!replication)
        {
            WalRcvExecResult* outcome;
            fixed (byte* command = Worf.ToServerCString(OrdinarySettings))
            {
                outcome = functions->walrcv_exec.Invoke(conn, (sbyte*)command, 0, null);
            }

            if (outcome->status != WalRcvExecStatus.WALRCV_OK_COMMAND)
            {
                string error = outcome->err is null ? "unknown error" : Worf.FromServer(outcome->err);
                NativeMethods.walrcv_clear_result(outcome);
                functions->walrcv_disconnect.Invoke(conn);
                throw new PgException(PgSqlStates.ConnectionFailure, "could not configure the connection: " + error);
            }

            NativeMethods.walrcv_clear_result(outcome);
        }

        MemoryContextData* scratch;
        fixed (byte* name = "worf connection\0"u8)
        {
            // The name is static, as a memory context's has to be.
            scratch = NativeMethods.AllocSetContextCreateInternal(NativeGlobals.TopMemoryContext, (sbyte*)name, 0, 1024, 8 * 1024);
        }

        if (node.Id == 0)
        {
            WorfNotices.RouteByAddress(((OrdinaryWalReceiverConn*)conn)->StreamConn);
        }

        return new WorfConnection(conn, scratch, functions->walrcv_server_version.Invoke(conn));
    }

    /// <summary>
    /// Runs a command that returns no rows, such as <c>SET</c>, <c>BEGIN</c> or <c>DROP_REPLICATION_SLOT</c>.
    /// </summary>
    /// <param name="command">The command text.</param>
    internal void Execute(string command)
    {
        WalRcvExecStatus status = Run(command, 0, null);
        if (status != WalRcvExecStatus.WALRCV_OK_COMMAND)
        {
            throw new PgException(PgSqlStates.ProtocolViolation, $"The replication command returned status {(int)status}; a completed command was expected.");
        }
    }

    /// <summary>
    /// Runs a query and returns its rows as text, as the server formats each value. Rows need a transaction, because
    /// libpqwalreceiver looks up their column types in the catalog.
    /// </summary>
    /// <param name="query">The query text.</param>
    /// <param name="columns">The number of columns the query returns.</param>
    /// <returns>The rows.</returns>
    internal IReadOnlyList<string?[]> Query(string query, int columns)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(columns, 1);
        List<string?[]> rows = [];
        WalRcvExecStatus status = Run(query, columns, rows);
        if (status == WalRcvExecStatus.WALRCV_OK_COMMAND)
        {
            return [];
        }

        if (status != WalRcvExecStatus.WALRCV_OK_TUPLES)
        {
            throw new PgException(PgSqlStates.ProtocolViolation, $"The replication query returned status {(int)status}; a row result was expected.");
        }

        return rows;
    }

    /// <summary>
    /// Starts a copy: <c>COPY ... TO STDOUT</c>, whose rows <see cref="Receive"/> then returns, or
    /// <c>START_REPLICATION</c>, which streams changes and accepts <see cref="Send"/>.
    /// </summary>
    /// <param name="command">The command text.</param>
    internal void StartCopy(string command)
    {
        WalRcvExecStatus status = Run(command, 0, null);
        if (status is not (WalRcvExecStatus.WALRCV_OK_COPY_OUT or WalRcvExecStatus.WALRCV_OK_COPY_BOTH))
        {
            throw new PgException(PgSqlStates.ProtocolViolation, $"The replication command returned status {(int)status}; a copy was expected.");
        }

        LastReceivedAt = NativeMethods.GetCurrentTimestamp();
    }

    /// <summary>
    /// Receives the next message of a copy, waiting up to <paramref name="timeout"/> for one.
    /// </summary>
    /// <param name="timeout">How long to wait, <see cref="TimeSpan.Zero"/> to only check, or
    /// <see cref="Timeout.InfiniteTimeSpan"/> to wait until a message arrives or the latch is set.</param>
    /// <param name="message">The message, valid until the connection's next call.</param>
    /// <returns>Whether a message arrived, the wait timed out, the latch woke it, or the copy ended.</returns>
    internal PgReplicationReceiveStatus Receive(TimeSpan timeout, out ReadOnlySpan<byte> message)
    {
        if (_readAhead.TryDequeue(out byte[]? ahead))
        {
            _readAheadBytes -= ahead.Length;
            message = ahead;
            return PgReplicationReceiveStatus.Message;
        }

        if (_endedAhead)
        {
            message = default;
            return PgReplicationReceiveStatus.Ended;
        }

        return ReceiveFromServer(timeout, out message);
    }

    /// <summary>
    /// Waits for a while as the messages of a copy keep coming, reading them ahead of their turn: each that
    /// <paramref name="handle"/> takes is handled now, and the rest wait for <see cref="Receive"/> in order, up to
    /// <paramref name="maxBytes"/> of them, after which the wait reads no more.
    /// </summary>
    /// <param name="duration">How long to wait.</param>
    /// <param name="maxBytes">The most bytes of messages to hold.</param>
    /// <param name="handle">Handles a message now, returning whether it did, so that it isn't given again.</param>
    /// <param name="stop">Whether to stop waiting, as when the worker is asked to stop.</param>
    internal void ReadAhead(TimeSpan duration, long maxBytes, ReadAheadHandler handle, Func<bool> stop)
    {
        long until = NativeMethods.GetCurrentTimestamp() + (long)duration.TotalMicroseconds;
        while (!stop())
        {
            int remaining = Milliseconds(NativeMethods.TimestampDifferenceMilliseconds(NativeMethods.GetCurrentTimestamp(), until));
            if (remaining <= 0)
            {
                return;
            }

            if (_endedAhead || _readAheadBytes >= maxBytes)
            {
                int events = NativeMethods.WaitLatchOrSocket(NativeGlobals.MyLatch, WlLatchSet | WlExitOnPmDeath | WlTimeout, InvalidSocket,
                    Math.Min(remaining, 1000), PgWaitExtension);
                if ((events & WlLatchSet) != 0)
                {
                    NativeMethods.ResetLatch(NativeGlobals.MyLatch);
                }

                PgInterrupts.Check();
                continue;
            }

            PgReplicationReceiveStatus status = ReceiveFromServer(TimeSpan.FromMilliseconds(Math.Min(remaining, 1000)), out ReadOnlySpan<byte> message);
            if (status == PgReplicationReceiveStatus.Ended)
            {
                _endedAhead = true;
            }
            else if (status == PgReplicationReceiveStatus.Message && !handle(message))
            {
                _readAhead.Enqueue(message.ToArray());
                _readAheadBytes += message.Length;
            }
        }
    }

    private PgReplicationReceiveStatus ReceiveFromServer(TimeSpan timeout, out ReadOnlySpan<byte> message)
    {
        long milliseconds = timeout == Timeout.InfiniteTimeSpan ? -1 : (long)timeout.TotalMilliseconds;
        ArgumentOutOfRangeException.ThrowIfLessThan(milliseconds, -1L, nameof(timeout));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(milliseconds, int.MaxValue, nameof(timeout));
        WalReceiverConn* conn = Conn;
        long deadline = milliseconds > 0 ? NativeMethods.GetCurrentTimestamp() + (milliseconds * 1000) : 0;
        message = default;
        while (true)
        {
            PgSocket socket = InvalidSocket;
            sbyte* data = null;
            PgInterrupts.Check();
            int length = Functions->walrcv_receive.Invoke(conn, &data, &socket);
            if (length < 0)
            {
                return PgReplicationReceiveStatus.Ended;
            }

            if (length > 0)
            {
                if (length > _message.Length)
                {
                    _message = new byte[Math.Max(length, _message.Length * 2)];
                }

                // libpqwalreceiver's buffer is freed by the connection's next call, so the message is copied first.
                new ReadOnlySpan<byte>(data, length).CopyTo(_message);
                message = _message.AsSpan(0, length);
                LastReceivedAt = NativeMethods.GetCurrentTimestamp();
                return PgReplicationReceiveStatus.Message;
            }

            if (milliseconds == 0)
            {
                return PgReplicationReceiveStatus.TimedOut;
            }

            int remaining = -1;
            if (milliseconds > 0)
            {
                remaining = Milliseconds(NativeMethods.TimestampDifferenceMilliseconds(NativeMethods.GetCurrentTimestamp(), deadline));
                if (remaining <= 0)
                {
                    return PgReplicationReceiveStatus.TimedOut;
                }
            }

            int events = NativeMethods.WaitLatchOrSocket(NativeGlobals.MyLatch,
                WlSocketReadable | WlLatchSet | WlExitOnPmDeath | (remaining >= 0 ? WlTimeout : 0), socket, remaining, PgWaitExtension);
            if ((events & WlLatchSet) != 0)
            {
                NativeMethods.ResetLatch(NativeGlobals.MyLatch);
                PgInterrupts.Check();
                return PgReplicationReceiveStatus.Woken;
            }

            if ((events & WlSocketReadable) == 0)
            {
                return PgReplicationReceiveStatus.TimedOut;
            }
        }
    }

    /// <summary>
    /// Sends a message during a streaming copy, such as a standby status update.
    /// </summary>
    /// <param name="message">The message bytes.</param>
    internal void Send(ReadOnlySpan<byte> message)
    {
        WalReceiverConn* conn = Conn;
        byte empty = 0;
        fixed (byte* address = message)
        {
            Functions->walrcv_send.Invoke(conn, address is null ? (sbyte*)&empty : (sbyte*)address, message.Length);
        }
    }

    /// <summary>
    /// Reads the rows of a <c>COPY ... TO STDOUT</c> started with <see cref="StartCopy"/> into a local table, as the
    /// built-in subscriber's table synchronization does: through PostgreSQL's own <c>COPY FROM</c>, which checks
    /// constraints, fills indexes, routes rows to partitions and fires triggers as <c>session_replication_role</c>
    /// selects. It needs <c>INSERT</c> on the table and, like the built-in subscriber, refuses a table whose row-level
    /// security applies to the user. If the copy fails, the server's copy is left unfinished, so dispose the
    /// connection.
    /// </summary>
    /// <param name="relationOid">The local table's OID.</param>
    /// <param name="columns">The table's columns that receive the copy's columns, in order, or null for all of them.</param>
    /// <returns>The number of rows copied.</returns>
    internal long CopyInto(uint relationOid, IReadOnlyList<string>? columns = null)
    {
        _ = Conn;
        if (!NativeMethods.IsTransactionState())
        {
            throw new PgException(PgSqlStates.InvalidTransactionState, "copying into a table requires a transaction");
        }

        List* attnamelist = null;
        foreach (string column in columns ?? [])
        {
            attnamelist = NativeMethods.lappend(attnamelist, NativeMethods.makeString(Worf.PStrDup(column)));
        }

        RelationData* relation = NativeMethods.table_open(relationOid, RowExclusiveLock);

        // tablesync.c makes the same checks: COPY FROM applies no row-level security policies.
        uint user = NativeMethods.GetUserId();
        if (NativeMethods.pg_class_aclcheck(relationOid, user, AclInsert) != AclResult.ACLCHECK_OK)
        {
            NativeMethods.aclcheck_error(AclResult.ACLCHECK_NO_PRIV, ObjectType.OBJECT_TABLE, (sbyte*)&relation->rd_rel->relname);
        }

        if (NativeMethods.check_enable_rls(relationOid, 0, false) == RlsEnabled)
        {
            throw new PgException(PgSqlStates.FeatureNotSupported,
                $"user \"{Worf.FromServer(NativeMethods.GetUserNameFromId(user, true))}\" cannot replicate into relation with row-level security enabled: \"{Worf.FromServer((sbyte*)&relation->rd_rel->relname)}\"");
        }

        bool snapshot = !NativeMethods.ActiveSnapshotSet();
        if (snapshot)
        {
            NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());
        }

        ParseState* pstate = NativeMethods.make_parsestate(null);
        _ = NativeMethods.addRangeTableEntryForRelation(pstate, relation, AccessShareLock, null, false, false);
        SetCopySource(this);
        try
        {
            CopyFromStateData* cstate;
            ulong rows;

            // The defaults of the columns the provider doesn't send name the sequences they draw from.
            SequenceRangeGuard.StartingCopy(relation, attnamelist);
            try
            {
                cstate = NativeMethods.BeginCopyFrom(pstate, relation, null, null, false, CopySource, attnamelist, Worf.CopyFromOptions());
                rows = NativeMethods.CopyFrom(cstate);
            }
            finally
            {
                SequenceRangeGuard.StartedExpressions();
            }

            NativeMethods.EndCopyFrom(cstate);

            // Text COPY can stop at an end-of-data line; data after it would leave the connection mid-copy.
            byte extra;
            if (ReadCopy(&extra, 1, 1) != 0)
            {
                throw new PgException(PgSqlStates.ProtocolViolation, "the copy sent data after its end-of-data marker");
            }

            if (snapshot)
            {
                NativeMethods.PopActiveSnapshot();
            }

            NativeMethods.free_parsestate(pstate);
            NativeMethods.table_close(relation, NoLock);
            return checked((long)rows);
        }
        finally
        {
            SetCopySource(null);
        }
    }

    /// <summary>
    /// Limits a number of milliseconds, which C's long holds, to an int, as a wait's timeout takes it.
    /// </summary>
    private static int Milliseconds(long milliseconds) => (int)Math.Min(milliseconds, int.MaxValue);

    /// <summary>
    /// Makes a connection the copy <see cref="ReadCopy"/> reads, with nothing read of it yet, or none.
    /// </summary>
    private static void SetCopySource(WorfConnection? source)
    {
        s_copySource = source;
        s_copyPending = null;
        s_copyPendingLength = 0;
        s_copyEnded = false;
    }

    /// <summary>
    /// Ends a streaming copy started with <c>START_REPLICATION</c>, so the connection runs commands again.
    /// </summary>
    internal void EndStreaming()
    {
        uint timeline = 0;
        Functions->walrcv_endstreaming.Invoke(Conn, &timeline);
    }

    /// <summary>
    /// Closes the connection.
    /// </summary>
    public void Dispose()
    {
        if (_conn is null)
        {
            return;
        }

        WalReceiverConn* conn = Conn;
        _conn = null;
        NativeMethods.MemoryContextDelete(_scratch);
        WorfNotices.Forget(((OrdinaryWalReceiverConn*)conn)->StreamConn);
        Functions->walrcv_disconnect.Invoke(conn);
    }

    /// <summary>
    /// Mirrors tablesync.c's copy_read_data: hands COPY the stream's bytes, waiting on the socket and latch, and
    /// returns fewer than minread bytes only at the end of the stream. It fills COPY's buffer with every message
    /// already received, rather than return after each, which is a row, so that COPY isn't called back once a row.
    /// </summary>
    private static int ReadCopy(void* output, int minread, int maxread)
    {
        int read = 0;
        while (maxread > 0)
        {
            if (s_copyPendingLength > 0)
            {
                int take = Math.Min(s_copyPendingLength, maxread);
                new ReadOnlySpan<byte>(s_copyPending, take).CopyTo(new Span<byte>((byte*)output + read, take));
                s_copyPending += take;
                s_copyPendingLength -= take;
                read += take;
                maxread -= take;
                continue;
            }

            if (s_copyEnded)
            {
                break;
            }

            PgInterrupts.Check();
            PgSocket socket = InvalidSocket;
            sbyte* data = null;
            int length = Functions->walrcv_receive.Invoke(s_copySource!.Conn, &data, &socket);
            if (length > 0)
            {
                s_copyPending = (byte*)data;
                s_copyPendingLength = length;
            }
            else if (length < 0)
            {
                s_copyEnded = true;
            }
            else if (read >= minread)
            {
                break;
            }
            else
            {
                _ = NativeMethods.WaitLatchOrSocket(NativeGlobals.MyLatch, WlSocketReadable | WlLatchSet | WlExitOnPmDeath, socket, -1,
                    PgWaitExtension);
                NativeMethods.ResetLatch(NativeGlobals.MyLatch);
            }
        }

        return read;
    }

    /// <summary>
    /// Runs one command, asking for text columns, and throws a remote failure; the rows of a query go to
    /// <paramref name="rows"/>.
    /// </summary>
    private WalRcvExecStatus Run(string text, int columns, List<string?[]>? rows)
    {
        WalReceiverConn* conn = Conn;

        // libpqwalreceiver looks up the result's column types in the catalog, which needs a transaction.
        if (columns > 0 && !NativeMethods.IsTransactionState())
        {
            throw new PgException(PgSqlStates.InvalidTransactionState, "a replication query that returns rows requires a transaction");
        }

        // Outside a transaction the current context lasts as long as the worker, so everything a command allocates
        // goes to the scratch context.
        NativeMethods.MemoryContextReset(_scratch);
        MemoryContextData* previous = Worf.MemoryContextSwitchTo(_scratch);
        try
        {
            uint* types = stackalloc uint[Math.Max(columns, 1)];
            for (int i = 0; i < columns; i++)
            {
                types[i] = TextOid;
            }

            WalRcvExecResult* outcome;
            fixed (byte* query = Worf.ToServerCString(text))
            {
                outcome = Functions->walrcv_exec.Invoke(conn, (sbyte*)query, columns, columns > 0 ? types : null);
            }

            WalRcvExecStatus status = outcome->status;
            if (status == WalRcvExecStatus.WALRCV_ERROR)
            {
                string? sqlState = outcome->sqlstate != 0 ? Worf.FromServer(NativeMethods.unpack_sql_state(outcome->sqlstate)) : null;
                string message = outcome->err is null ? "The command failed on the server." : Worf.FromServer(outcome->err);
                NativeMethods.walrcv_clear_result(outcome);
                throw new PgRemoteException(sqlState, PrimaryMessage(message));
            }

            if (status == WalRcvExecStatus.WALRCV_OK_TUPLES && rows is not null)
            {
                TupleDescData* desc = outcome->tupledesc;
                if (desc->natts != columns)
                {
                    NativeMethods.walrcv_clear_result(outcome);
                    throw new InvalidOperationException("Invalid replication query result.");
                }

                TupleTableSlot* slot = NativeMethods.MakeSingleTupleTableSlot(desc, NativeGlobals.DangerousAddressOf_TTSOpsMinimalTuple());
                while (NativeMethods.tuplestore_gettupleslot(outcome->tuplestore, true, false, slot))
                {
                    string?[] values = new string?[columns];
                    for (int i = 0; i < columns; i++)
                    {
                        bool isnull;
                        ulong value = NativeMethods.slot_getattr(slot, i + 1, &isnull);
                        values[i] = isnull ? null : Worf.FromServer(NativeMethods.text_to_cstring((varlena*)value));
                    }

                    rows.Add(values);
                    _ = NativeMethods.ExecClearTuple(slot);
                }

                NativeMethods.ExecDropSingleTupleTableSlot(slot);
            }

            NativeMethods.walrcv_clear_result(outcome);
            return status;
        }
        finally
        {
            _ = Worf.MemoryContextSwitchTo(previous);
        }
    }

    /// <summary>
    /// Removes the severity label libpq puts before a server's message, such as <c>ERROR:  </c>, and trailing lines.
    /// </summary>
    private static string PrimaryMessage(string message)
    {
        int label = message.IndexOf(":  ", StringComparison.Ordinal);
        if (label > 0 && message.AsSpan(0, label).IndexOfAnyExceptInRange('A', 'Z') < 0)
        {
            message = message[(label + 3)..];
        }

        return message.TrimEnd();
    }

    /// <summary>
    /// Connects as libpqwalreceiver does, through the libpq it loaded, for logical replication or for SQL and copies, and
    /// hands the connection to libpqwalreceiver in the layout its WalReceiverConn has kept since PostgreSQL 10, so that
    /// its query, copy and disconnect functions run on it. worf connects itself, rather than through walrcv_connect, so
    /// that the notices the server sends as the connection logs in, as warnings of a setting that can't apply, go to this
    /// server's log as the rest do, rather than bare to standard error; so that each address gets the connection's
    /// connect_timeout, which libpq's own polling doesn't keep, and a host name that is slow to look up holds up none
    /// after it, as <see cref="WorfProbe"/> connects; and as before PostgreSQL 17, libpqwalreceiver connects only for
    /// replication. A logical replication connection asks for this database's encoding and settings that make the values
    /// it gets unambiguous, after the connection string's own options, and searches no schema, as libpqwalreceiver's do.
    /// </summary>
    private static WalReceiverConn* ConnectLibpq(sbyte* conninfo, sbyte* application, bool replication, (string Name, uint Id) node, sbyte** failure)
    {
        // A logical replication connection runs SQL, so it searches no schema whose owner could have it run their code.
        void* pgconn;
        using (WorfProbe probe = WorfProbe.Start(Worf.FromServer(conninfo), Worf.FromServer(application), (node.Id == 0 ? "a server" : $"node {node.Name}", node.Id),
            replication ? "SELECT pg_catalog.set_config('search_path', '', false);" : "", long.MaxValue, replication, (WaitLibpqConnect, WaitLibpqReceive)))
        {
            while (!probe.Done)
            {
                if (probe.Wait(1000))
                {
                    PgInterrupts.Check();
                }

                probe.Advance();
            }

            if (probe.Failure is string failed)
            {
                *failure = Worf.PStrDup(failed);
                return null;
            }

            pgconn = probe.TakeLibpq();
        }

        OrdinaryWalReceiverConn* conn = (OrdinaryWalReceiverConn*)NativeMethods.MemoryContextAllocZero(NativeGlobals.TopMemoryContext,
            (ulong)sizeof(OrdinaryWalReceiverConn));
        conn->StreamConn = pgconn;
        conn->Logical = replication;
        return (WalReceiverConn*)conn;
    }

    /// <summary>
    /// Gets the options libpq would connect with, from the connection string and, for those it doesn't give, a service
    /// file or the environment: libpq takes them all in as it starts a connection, before it looks a host up, and stops
    /// there at the session setting, which no server takes.
    /// </summary>
    /// <param name="conninfo">The connection string.</param>
    /// <param name="applicationName">The name the server would show for the connection.</param>
    /// <returns>The options that have values, or null.</returns>
    internal static Dictionary<string, string>? EffectiveOptions(string conninfo, string applicationName)
    {
        var start = (delegate* unmanaged<sbyte**, sbyte**, int, void*>)LibpqFunction("PQconnectStartParams");
        var finish = (delegate* unmanaged<void*, void>)LibpqFunction("PQfinish");
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
            byte* options = (byte*)((delegate* unmanaged<void*, void*>)LibpqFunction("PQconninfo"))(conn);
            return options is null ? null : Options(options).ToDictionary(option => option.Keyword, option => option.Value, StringComparer.Ordinal);
        }
        finally
        {
            finish(conn);
        }
    }

    /// <summary>
    /// Parses a connection string, or a URI, into the options it gives, as libpq's PQconninfoParse does, in the order of
    /// libpq's option table.
    /// </summary>
    /// <param name="conninfo">The connection string.</param>
    /// <returns>The options, or null for one libpq can't parse.</returns>
    internal static List<(string Keyword, string Value)>? ParseConninfo(string conninfo)
    {
        var parse = (delegate* unmanaged<sbyte*, sbyte**, byte*>)LibpqFunction("PQconninfoParse");
        var free = (delegate* unmanaged<void*, void>)LibpqFunction("PQfreemem");
        byte[] text = Worf.ToServerCString(conninfo);
        sbyte* failure = null;
        byte* options;
        fixed (byte* textAddress = text)
        {
            options = parse((sbyte*)textAddress, &failure);
        }

        if (failure is not null)
        {
            free(failure);
        }

        return options is null ? null : Options(options);
    }

    // The options of a PQconninfoOption array that have values, which it frees: each its keyword, environment variable,
    // compiled default, value, label and display character, and its display size, to a keyword of null.
    private static List<(string Keyword, string Value)> Options(byte* options)
    {
        int stride = ((6 * sizeof(nint)) + sizeof(int) + sizeof(nint) - 1) / sizeof(nint) * sizeof(nint);
        List<(string Keyword, string Value)> found = [];
        try
        {
            for (byte* option = options; *(sbyte**)option is not null; option += stride)
            {
                sbyte* value = *(sbyte**)(option + (3 * sizeof(nint)));
                if (value is not null)
                {
                    found.Add((Worf.FromServer(*(sbyte**)option), Worf.FromServer(value)));
                }
            }
        }
        finally
        {
            ((delegate* unmanaged<void*, void>)LibpqFunction("PQconninfoFree"))(options);
        }

        return found;
    }

    /// <summary>
    /// Takes an ordinary connection that libpq has made and set up, as a <see cref="WorfProbe"/> makes one, handing it to
    /// libpqwalreceiver in the layout its WalReceiverConn has kept since PostgreSQL 10, so that its query, copy and
    /// disconnect functions run on it.
    /// </summary>
    /// <param name="pgconn">The libpq connection, which the connection owns from now on.</param>
    /// <returns>The connection.</returns>
    internal static WorfConnection FromLibpq(void* pgconn)
    {
        WalReceiverFunctionsType* functions = Functions;
        OrdinaryWalReceiverConn* conn = (OrdinaryWalReceiverConn*)NativeMethods.MemoryContextAllocZero(NativeGlobals.TopMemoryContext,
            (ulong)sizeof(OrdinaryWalReceiverConn));
        conn->StreamConn = pgconn;
        MemoryContextData* scratch;
        fixed (byte* name = "worf connection\0"u8)
        {
            scratch = NativeMethods.AllocSetContextCreateInternal(NativeGlobals.TopMemoryContext, (sbyte*)name, 0, 1024, 8 * 1024);
        }

        return new WorfConnection((WalReceiverConn*)conn, scratch, functions->walrcv_server_version.Invoke((WalReceiverConn*)conn));
    }

    /// <summary>
    /// Makes a socket of the descriptor libpq's <c>PQsocket</c> returns, as C converts it.
    /// </summary>
    /// <param name="descriptor">The descriptor.</param>
    /// <returns>The socket.</returns>
#if WORF_WINDOWS
    internal static PgSocket ToSocket(int descriptor) => unchecked((PgSocket)descriptor);
#else
    internal static PgSocket ToSocket(int descriptor) => descriptor;
#endif

    /// <summary>
    /// Finds a function of the libpq that libpqwalreceiver uses: PostgreSQL's dynamic loader searches
    /// libpqwalreceiver and the libraries it depends on; on Windows a module's exports exclude its dependencies, so
    /// libpq is found by name.
    /// </summary>
    /// <param name="name">The function's name.</param>
    /// <returns>Its address.</returns>
    internal static void* LibpqFunction(string name)
    {
        void* function = null;
        if (OperatingSystem.IsWindows())
        {
            if (NativeLibrary.TryLoad("libpq.dll", out nint library) && NativeLibrary.TryGetExport(library, name, out nint address))
            {
                function = (void*)address;
            }
        }
        else
        {
            fixed (byte* library = "libpqwalreceiver\0"u8)
            {
                fixed (byte* symbol = Worf.ToServerCString(name))
                {
                    function = NativeMethods.load_external_function((sbyte*)library, (sbyte*)symbol, false, null);
                }
            }
        }

        return function is not null
            ? function
            : throw new PgException(PgSqlStates.UndefinedFunction, $"could not find the libpq function \"{name}\" that libpqwalreceiver uses");
    }

    private const uint TextOid = 25;

    // PGINVALID_SOCKET: INVALID_SOCKET on Windows, -1 elsewhere.
#if WORF_WINDOWS
    private const PgSocket InvalidSocket = ulong.MaxValue;
#else
    private const PgSocket InvalidSocket = -1;
#endif
    private const int RowExclusiveLock = 3;
    private const int AccessShareLock = 1;
    private const int NoLock = 0;
    private const uint AclInsert = 1;
    private const int RlsEnabled = 2;
}
