using Ankus.Postgres;
#if WORF_WINDOWS
using PgSocket = ulong;
#else
using PgSocket = int;
#endif

namespace Worf;

/// <summary>
/// A query sent on a libpq connection that the exchange worker doesn't wait on: libpq, in its nonblocking mode, sends
/// what the socket takes at once, and the worker sends the rest and reads the results as the socket lets it, between its
/// other work, so that a neighbor that stops reading holds up no other call however large the query. The query returns
/// one text value, as a call of a function does. libpq's result values are part of its stable interface.
/// </summary>
internal sealed unsafe class WorfPendingQuery
{
    private const int WlSocketReadable = 2;
    private const int WlSocketWriteable = 4;
    private const int WlTimeout = 8;
    private const int WlExitOnPmDeath = 32;
    private const int WlLatchSet = 1;
    private const uint PgWaitExtension = 0x07000000;

    // CONNECTION_BAD, PGRES_COMMAND_OK and PGRES_TUPLES_OK, and the fields of an error result: its SQLSTATE and its
    // primary message.
    private const int ConnectionBad = 1;
    private const int CommandOk = 1;
    private const int TuplesOk = 2;
    private const int DiagSqlState = 'C';
    private const int DiagMessagePrimary = 'M';

    private readonly void* _conn;
    private bool _flushing;

    private WorfPendingQuery(void* conn, long deadline)
    {
        _conn = conn;
        Deadline = deadline;
    }

    /// <summary>
    /// Gets when the query runs out of time, as a timestamp.
    /// </summary>
    internal long Deadline { get; }

    /// <summary>
    /// Gets a value indicating whether the query has ended, with its value or its failure.
    /// </summary>
    internal bool Done { get; private set; }

    /// <summary>
    /// Gets the value the query returned, its first row's first column, or null.
    /// </summary>
    internal string? Value { get; private set; }

    /// <summary>
    /// Gets why the query failed, or null.
    /// </summary>
    internal string? Failure { get; private set; }

    /// <summary>
    /// Gets the SQLSTATE of the query's failure on the server, or null.
    /// </summary>
    internal string? SqlState { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the connection failed, rather than the query on the server.
    /// </summary>
    internal bool Broken { get; private set; }

    /// <summary>
    /// Sends a query.
    /// </summary>
    /// <param name="connection">The connection, on which no other query runs meanwhile.</param>
    /// <param name="sql">The query.</param>
    /// <param name="deadline">When the query runs out of time, as a timestamp.</param>
    /// <returns>The query.</returns>
    internal static WorfPendingQuery Start(WorfConnection connection, string sql, long deadline)
    {
        void* conn = connection.Libpq;
        WorfPendingQuery query = new(conn, deadline);
        if (((delegate* unmanaged<void*, int, int>)WorfConnection.LibpqFunction("PQsetnonblocking"))(conn, 1) != 0)
        {
            query.Fail(null);
            return query;
        }

        fixed (byte* text = Worf.ToServerCString(sql))
        {
            if (((delegate* unmanaged<void*, sbyte*, int>)WorfConnection.LibpqFunction("PQsendQuery"))(conn, (sbyte*)text) == 0)
            {
                query.Fail(null);
                return query;
            }
        }

        query._flushing = true;
        query.Advance();
        return query;
    }

    /// <summary>
    /// Reads what the server has sent of the query's results, without waiting.
    /// </summary>
    internal void Advance()
    {
        if (Done)
        {
            return;
        }

        // What the socket hasn't taken of the query yet goes as it takes more; the server's answers are read meanwhile,
        // as libpq asks.
        if (((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQconsumeInput"))(_conn) == 0)
        {
            Fail(null);
            return;
        }

        if (_flushing)
        {
            int flushed = ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQflush"))(_conn);
            if (flushed < 0)
            {
                Fail(null);
                return;
            }

            _flushing = flushed > 0;
            if (_flushing)
            {
                return;
            }
        }

        var busy = (delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQisBusy");
        var getResult = (delegate* unmanaged<void*, void*>)WorfConnection.LibpqFunction("PQgetResult");
        var resultStatus = (delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQresultStatus");
        var clear = (delegate* unmanaged<void*, void>)WorfConnection.LibpqFunction("PQclear");
        while (busy(_conn) == 0)
        {
            void* result = getResult(_conn);
            if (result is null)
            {
                End();
                return;
            }

            int status = resultStatus(result);
            if (status == TuplesOk && Value is null && Failure is null)
            {
                var tuples = (delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQntuples");
                var fields = (delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQnfields");
                var isNull = (delegate* unmanaged<void*, int, int, int>)WorfConnection.LibpqFunction("PQgetisnull");
                var getValue = (delegate* unmanaged<void*, int, int, sbyte*>)WorfConnection.LibpqFunction("PQgetvalue");
                if (tuples(result) > 0 && fields(result) > 0 && isNull(result, 0, 0) == 0)
                {
                    Value = Worf.FromServer(getValue(result, 0, 0));
                }
            }
            else if (status != CommandOk && status != TuplesOk && Failure is null)
            {
                var field = (delegate* unmanaged<void*, int, sbyte*>)WorfConnection.LibpqFunction("PQresultErrorField");
                sbyte* state = field(result, DiagSqlState);
                sbyte* primary = field(result, DiagMessagePrimary);
                SqlState = state is null ? null : Worf.FromServer(state);
                Failure = primary is null
                    ? Worf.FromServer(((delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction("PQresultErrorMessage"))(result)).TrimEnd()
                    : Worf.FromServer(primary);
            }

            clear(result);
        }

        if (((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQstatus"))(_conn) == ConnectionBad)
        {
            Fail(null);
        }
    }

    /// <summary>
    /// Waits up to a time for the connection's socket to take more of the query, or to have more of the results, or for
    /// the latch.
    /// </summary>
    /// <param name="milliseconds">The time.</param>
    /// <returns>Whether the latch was set, which the wait resets.</returns>
    internal bool Wait(long milliseconds)
    {
        int descriptor = ((delegate* unmanaged<void*, int>)WorfConnection.LibpqFunction("PQsocket"))(_conn);
        if (descriptor < 0)
        {
            Fail(null);
            return false;
        }

        PgSocket socket = WorfConnection.ToSocket(descriptor);
        int events = NativeMethods.WaitLatchOrSocket(NativeGlobals.MyLatch,
            WlExitOnPmDeath | WlLatchSet | WlTimeout | WlSocketReadable | (_flushing ? WlSocketWriteable : 0), socket, Math.Max(milliseconds, 0), PgWaitExtension);
        if ((events & WlLatchSet) == 0)
        {
            return false;
        }

        NativeMethods.ResetLatch(NativeGlobals.MyLatch);
        return true;
    }

    // Ends the query as failed on the connection, with libpq's message when none is given.
    private void Fail(string? failure)
    {
        Failure ??= failure ?? Worf.FromServer(((delegate* unmanaged<void*, sbyte*>)WorfConnection.LibpqFunction("PQerrorMessage"))(_conn)).TrimEnd();
        Broken = true;
        End();
    }

    // Ends the query, leaving the connection blocking again, as its other users take it.
    private void End()
    {
        _flushing = false;
        _ = ((delegate* unmanaged<void*, int, int>)WorfConnection.LibpqFunction("PQsetnonblocking"))(_conn, 0);
        Done = true;
    }
}
