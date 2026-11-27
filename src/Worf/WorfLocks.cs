using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The locks worf takes of its own: advisory locks of the database, which show in pg_locks with objsubid 5 for the
/// node's exchange turn, 7 for the building of an exchange's message and 8 for the reservation of the Snowflake clock,
/// which the keys of pg_advisory_lock and pg_advisory_lock_shared, whose objsubid is 1 or 2, never have, so that no
/// session's advisory lock holds them up, as the switch gates' 3 and 4 don't either.
/// </summary>
internal static class WorfLocks
{
    // ExclusiveLock from storage/lockdefs.h, LOCKTAG_ADVISORY and DEFAULT_LOCKMETHOD from storage/lock.h, and
    // LOCKACQUIRE_NOT_AVAIL.
    private const int ExclusiveLock = 7;
    private const byte LockTagAdvisory = 10;
    private const byte DefaultLockMethod = 1;
    private const int NotAvailable = 0;
    private const ushort TurnKey = 5;
    private const ushort BuildKey = 7;
    private const ushort ClockKey = 8;

    /// <summary>
    /// Waits for the node's exchange turn, which the transaction holds until it ends.
    /// </summary>
    internal static unsafe void TakeTurn()
    {
        LOCKTAG turn = Tag(0, TurnKey);
        _ = NativeMethods.LockAcquire(&turn, ExclusiveLock, false, false);
    }

    /// <summary>
    /// Runs work in the node's exchange turn, held for the session and let go as the work ends, however it ends, rather
    /// than as the transaction does, which a session lock outlives. Work that fails may leave PostgreSQL out of reach as
    /// it ends: within a subtransaction until that has rolled back, and after a cancel, such as statement_timeout's,
    /// until the statement has ended. The turn is then owed, and let go as the subtransaction or the transaction ends,
    /// or before the next turn is taken.
    /// </summary>
    /// <param name="work">The work.</param>
    internal static void WithTurn(Action work)
    {
        TakeTurnForSession();
        try
        {
            work();
        }
        finally
        {
            s_turnsOwed++;
            LeaveTurns();
        }
    }

    /// <summary>
    /// Runs work in a subtransaction, in the node's exchange turn, for a caller that tries again when the work fails:
    /// the turn is taken within the subtransaction, so that a wait for it that fails is one more try, and let go once
    /// the subtransaction has ended, as PostgreSQL takes no call from one that failed until it has rolled back.
    /// </summary>
    /// <param name="work">The work.</param>
    internal static void WithTurnInSubtransaction(Action work)
    {
        bool taken = false;
        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                TakeTurnForSession();
                taken = true;
                work();
            });
        }
        finally
        {
            if (taken)
            {
                s_turnsOwed++;
                LeaveTurns();
            }
        }
    }

    // The turns this session took whose work has ended, and whether this transaction's end lets them go.
    private static int s_turnsOwed;
    private static bool s_turnEnds;

    // Takes the node's exchange turn for the session, once the turns owed are let go.
    private static unsafe void TakeTurnForSession()
    {
        LeaveTurns();
        if (!s_turnEnds)
        {
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () => EndTurns(false));
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, static () => EndTurns(false));
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, static () => EndTurns(true));
            _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort, static (_, _) => EndTurns(false, true));
            s_turnEnds = true;
        }

        LOCKTAG turn = Tag(0, TurnKey);
        _ = NativeMethods.LockAcquire(&turn, ExclusiveLock, true, false);
    }

    // Lets go the turns whose work has ended. Each is counted as owed until PostgreSQL has let it go.
    private static unsafe void LeaveTurns()
    {
        while (s_turnsOwed > 0)
        {
            LOCKTAG turn = Tag(0, TurnKey);
            _ = NativeMethods.LockRelease(&turn, ExclusiveLock, true);
            s_turnsOwed--;
        }
    }

    // Lets the turns owed go as a subtransaction rolls back or the transaction ends. An abort of the transaction lets
    // go every lock of the session's but those of pg_advisory_lock, so nothing is owed after it.
    private static void EndTurns(bool aborted, bool subtransaction = false)
    {
        if (!subtransaction)
        {
            s_turnEnds = false;
        }

        if (aborted)
        {
            s_turnsOwed = 0;
            return;
        }

        try
        {
            LeaveTurns();
        }
        catch (PgException failed)
        {
            WorfLog.Warning($"could not let the node's exchange turn go: {failed.Message}");
        }
    }

    /// <summary>
    /// Waits for this node's other exchanges to finish building their messages, which the transaction then holds until it
    /// ends.
    /// </summary>
    internal static unsafe void LockBuild()
    {
        LOCKTAG build = Tag(0, BuildKey);
        _ = NativeMethods.LockAcquire(&build, ExclusiveLock, false, false);
    }

    /// <summary>
    /// Takes the lock of the Snowflake clock's reservation for the session, unless another process holds it.
    /// </summary>
    /// <returns>Whether it took it.</returns>
    internal static unsafe bool TryLockClock()
    {
        LOCKTAG clock = Tag(0, ClockKey);
        return NativeMethods.LockAcquire(&clock, ExclusiveLock, true, true) != NotAvailable;
    }

    /// <summary>
    /// Lets the session's lock of the Snowflake clock's reservation go.
    /// </summary>
    internal static unsafe void UnlockClock()
    {
        LOCKTAG clock = Tag(0, ClockKey);
        _ = NativeMethods.LockRelease(&clock, ExclusiveLock, true);
    }

    /// <summary>
    /// Gets when the client's statement_timeout ends the statement, as a timestamp: as it started, which PostgreSQL arms
    /// the timeout at, or a moment before, and statement_timeout after. A background worker, whose statements PostgreSQL
    /// arms none for, and a statement with no timeout, have none.
    /// </summary>
    /// <returns>The deadline, or null.</returns>
    internal static long? StatementDeadline()
    {
        unsafe
        {
            return NativeGlobals.MyBackendType == BackendType.B_BACKEND && NativeGlobals.StatementTimeout > 0
                ? NativeMethods.GetCurrentStatementStartTimestamp() + (NativeGlobals.StatementTimeout * 1000L)
                : null;
        }
    }

    /// <summary>
    /// Ends the statement for its statement_timeout, as a lock timeout is caught, once the timeout's time has passed. A lock
    /// timeout and the statement's timeout that come due at the same alarm, as a timer coarser than the lock timeout's
    /// millisecond has them, as Windows' does, end the statement with whichever was due first, and clear both, so a lock
    /// timeout caught to go on would leave the statement running past its own timeout.
    /// </summary>
    internal static void EndIfTimedOut()
    {
        if (StatementDeadline() is long at && Worker.GetCurrentTimestamp() >= at)
        {
            throw new PgException(PgSqlStates.QueryCanceled, "canceling statement due to statement timeout");
        }
    }

    /// <summary>
    /// Sets a setting at the GUC nest level just made, as a function's SET clause does, so that AtEOXact_GUC at that
    /// level puts it back: a SET LOCAL made there would stay set for the enclosing level once the nested one commits.
    /// </summary>
    /// <param name="name">The setting.</param>
    /// <param name="value">Its value.</param>
    internal static unsafe void SetAtNestLevel(string name, string value)
    {
        fixed (byte* setting = Worf.ToServerCString(name))
        fixed (byte* set = Worf.ToServerCString(value))
        {
            _ = NativeMethods.set_config_option((sbyte*)setting, (sbyte*)set, GucContext.PGC_SUSET, GucSource.PGC_S_SESSION,
                GucAction.GUC_ACTION_SAVE, true, 0, false);
        }
    }

    private static LOCKTAG Tag(uint obj, ushort key)
    {
        uint database;
        unsafe
        {
            database = NativeGlobals.MyDatabaseId;
        }

        return new()
        {
            locktag_field1 = database,
            locktag_field2 = obj,
            locktag_field3 = 0,
            locktag_field4 = key,
            locktag_type = LockTagAdvisory,
            locktag_lockmethodid = DefaultLockMethod,
        };
    }
}
