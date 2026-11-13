using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The locks worf takes of its own: advisory locks of the database, which show in pg_locks with objsubid 5 for the
/// node's exchange turn, 6, with the subscription's OID, for a drop's lock, 7 for the building of an exchange's message
/// and 8 for the reservation of the Snowflake clock, which the keys of pg_advisory_lock and pg_advisory_lock_shared, whose
/// objsubid is 1 or 2, never have, so that no session's advisory lock holds them up, as the switch gates' 3 and 4 don't
/// either.
/// </summary>
internal static class WorfLocks
{
    // ShareLock and ExclusiveLock from storage/lockdefs.h, LOCKTAG_ADVISORY and DEFAULT_LOCKMETHOD from storage/lock.h,
    // and LOCKACQUIRE_NOT_AVAIL.
    private const int ShareLock = 5;
    private const int ExclusiveLock = 7;
    private const byte LockTagAdvisory = 10;
    private const byte DefaultLockMethod = 1;
    private const int NotAvailable = 0;
    private const ushort TurnKey = 5;
    private const ushort DropKey = 6;
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
    /// Runs work in the node's exchange turn, held for the session and let go as the work ends, rather than as the
    /// transaction does, with interrupts held off, so that a cancel pending as the work ends, as statement_timeout's,
    /// doesn't leave the turn held for the rest of the session.
    /// </summary>
    /// <param name="work">The work.</param>
    internal static unsafe void WithTurn(Action work)
    {
        LOCKTAG turn = Tag(0, TurnKey);
        _ = NativeMethods.LockAcquire(&turn, ExclusiveLock, true, false);
        try
        {
            work();
        }
        finally
        {
            NativeGlobals.InterruptHoldoffCount++;
            try
            {
                _ = NativeMethods.LockRelease(&turn, ExclusiveLock, true);
            }
            finally
            {
                NativeGlobals.InterruptHoldoffCount--;
            }
        }
    }

    /// <summary>
    /// Takes a subscription's drop lock, which the transaction holds until it ends, before the drop deletes anything.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    internal static unsafe void LockDrop(uint subid)
    {
        LOCKTAG drop = Tag(subid, DropKey);
        _ = NativeMethods.LockAcquire(&drop, ExclusiveLock, false, false);
    }

    /// <summary>
    /// Takes a subscription's drop lock shared, as a reply's take-in does, unless a drop holds it.
    /// </summary>
    /// <param name="subid">The subscription.</param>
    /// <returns>Whether no drop holds it.</returns>
    internal static unsafe bool TryShareDrop(uint subid)
    {
        LOCKTAG drop = Tag(subid, DropKey);
        return (int)NativeMethods.LockAcquire(&drop, ShareLock, false, true) != NotAvailable;
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
        return (int)NativeMethods.LockAcquire(&clock, ExclusiveLock, true, true) != NotAvailable;
    }

    /// <summary>
    /// Lets the session's lock of the Snowflake clock's reservation go, with interrupts held off.
    /// </summary>
    internal static unsafe void UnlockClock()
    {
        LOCKTAG clock = Tag(0, ClockKey);
        NativeGlobals.InterruptHoldoffCount++;
        try
        {
            _ = NativeMethods.LockRelease(&clock, ExclusiveLock, true);
        }
        finally
        {
            NativeGlobals.InterruptHoldoffCount--;
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
