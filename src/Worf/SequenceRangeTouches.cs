using Ankus.Postgres;

namespace Worf;

/// <summary>
/// How often each managed sequence's ranges changed, or a session asked the range worker to check it, in shared memory,
/// so that the range worker checks in full only the sequences something happened to, rather than every sequence on any
/// change. Each sequence of the server has a counter of its own, which the range worker gives up as the sequence's drop
/// commits, up to the number of counters, past which a change to a sequence without one counts for every sequence, as a
/// change that names no sequence does.
/// </summary>
internal static class SequenceRangeTouches
{
    /// <summary>
    /// The number of counters, for every database of the server.
    /// </summary>
    internal const int MaxCounters = 16384;

    // An entry that a dropped sequence gave up: no database, and the largest OID.
    private const uint Freed = uint.MaxValue;

    // The sequences this transaction changed, which count once it commits or prepares, and whether it changed one it
    // doesn't name.
    private static readonly HashSet<uint> s_onCommit = [];
    private static bool s_allOnCommit;

    /// <summary>
    /// Gets the shared counters.
    /// </summary>
    internal static PgLwLock<WorfTouchTable, WorfTouch> Counters { get; } = new("worf_sequence_range_touches");

    /// <summary>
    /// Sets up the shared memory.
    /// </summary>
    internal static void ShmemInit() => PgSharedMemory.Initialize(Counters, MaxCounters, static () => default, static _ => default);

    /// <summary>
    /// Counts a change to a sequence's ranges, or a request to check it, now.
    /// </summary>
    /// <param name="seqoid">The sequence, or null for a change to every sequence.</param>
    internal static void Touch(uint? seqoid)
    {
        using PgLwLockExclusiveGuard<WorfTouchTable, WorfTouch> guard = Counters.Exclusive();
        Count(guard, seqoid);
    }

    /// <summary>
    /// Counts a change to a sequence's ranges once this transaction commits or prepares.
    /// </summary>
    /// <param name="seqoid">The sequence, or null for a change to every sequence.</param>
    internal static void OnCommit(uint? seqoid)
    {
        if (seqoid is uint one)
        {
            _ = s_onCommit.Add(one);
        }
        else
        {
            s_allOnCommit = true;
        }
    }

    /// <summary>
    /// Counts the changes this transaction made, as it commits or prepares.
    /// </summary>
    internal static void Commit()
    {
        if (s_allOnCommit || s_onCommit.Count > 0)
        {
            using PgLwLockExclusiveGuard<WorfTouchTable, WorfTouch> guard = Counters.Exclusive();
            if (s_allOnCommit)
            {
                Count(guard, null);
            }

            foreach (uint seqoid in s_onCommit)
            {
                Count(guard, seqoid);
            }
        }

        Forget();
    }

    /// <summary>
    /// Forgets the changes this transaction made, as it rolls back.
    /// </summary>
    internal static void Forget()
    {
        s_onCommit.Clear();
        s_allOnCommit = false;
    }

    /// <summary>
    /// Gets the counts of changes that count for a sequence: those that name no sequence, and its own.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The counts.</returns>
    internal static (long All, long Sequence) Touched(uint seqoid)
    {
        using PgLwLockShareGuard<WorfTouchTable, WorfTouch> guard = Counters.Share();
        uint dboid = DatabaseId();
        int slot = Find(i => guard[i], dboid, seqoid, out _);
        return (guard.Header.All, slot >= 0 ? guard[slot].Count : 0);
    }

    /// <summary>
    /// Gives up a dropped sequence's counter, as the range worker removes its range state.
    /// </summary>
    /// <param name="seqoid">The sequence, of this database.</param>
    internal static void Free(uint seqoid)
    {
        using PgLwLockExclusiveGuard<WorfTouchTable, WorfTouch> guard = Counters.Exclusive();
        int slot = Find(i => guard[i], DatabaseId(), seqoid, out _);
        if (slot >= 0)
        {
            guard[slot] = new WorfTouch { Dboid = 0, Seqoid = Freed, Count = 0 };
        }
    }

    private static void Count(PgLwLockExclusiveGuard<WorfTouchTable, WorfTouch> guard, uint? seqoid)
    {
        uint dboid = DatabaseId();
        int free = -1;
        int slot = seqoid is uint one ? Find(i => guard[i], dboid, one, out free) : -1;
        if (seqoid is uint taken && slot < 0 && free >= 0)
        {
            slot = free;
            guard[slot] = new WorfTouch { Dboid = dboid, Seqoid = taken, Count = 0 };
        }

        if (slot >= 0)
        {
            WorfTouch touch = guard[slot];
            touch.Count++;
            guard[slot] = touch;
        }
        else
        {
            WorfTouchTable header = guard.Header;
            header.All++;
            guard.Header = header;
        }
    }

    // Finds a sequence's counter, probing from its hash's slot until an entry never taken, and the first entry it could
    // take, free or given up, or -1 for none.
    private static int Find(Func<int, WorfTouch> entry, uint dboid, uint seqoid, out int free)
    {
        free = -1;
        int start = (int)(((dboid * 2654435761UL) ^ seqoid) % MaxCounters);
        for (int probe = 0; probe < MaxCounters; probe++)
        {
            int slot = (start + probe) % MaxCounters;
            WorfTouch touch = entry(slot);
            if (touch.Dboid == dboid && touch.Seqoid == seqoid)
            {
                return slot;
            }

            if (touch.Dboid == 0)
            {
                if (free < 0)
                {
                    free = slot;
                }

                if (touch.Seqoid != Freed)
                {
                    return -1;
                }
            }
        }

        return -1;
    }

    private static uint DatabaseId()
    {
        unsafe
        {
            return NativeGlobals.MyDatabaseId;
        }
    }
}
