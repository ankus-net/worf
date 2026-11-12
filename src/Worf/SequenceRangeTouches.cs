using Ankus.Postgres;

namespace Worf;

/// <summary>
/// How often each managed sequence's ranges changed, or a session asked the range worker to check it, in shared memory,
/// so that the range worker checks in full only the sequences something happened to, rather than every sequence on any
/// change. Sequences share counters by hash, and a change that names no sequence counts for every sequence.
/// </summary>
internal static class SequenceRangeTouches
{
    /// <summary>
    /// The number of counters, for every database of the server.
    /// </summary>
    internal const int MaxCounters = 1024;

    // The sequences this transaction changed, which count once it commits or prepares, and whether it changed one it
    // doesn't name.
    private static readonly HashSet<uint> s_onCommit = [];
    private static bool s_allOnCommit;

    /// <summary>
    /// Gets the shared counters.
    /// </summary>
    internal static PgLwLock<WorfTouchTable, long> Counters { get; } = new("worf_sequence_range_touches");

    /// <summary>
    /// Sets up the shared memory.
    /// </summary>
    internal static void ShmemInit() => PgSharedMemory.Initialize(Counters, MaxCounters, static () => default, static _ => 0L);

    /// <summary>
    /// Counts a change to a sequence's ranges, or a request to check it, now.
    /// </summary>
    /// <param name="seqoid">The sequence, or null for a change to every sequence.</param>
    internal static void Touch(uint? seqoid)
    {
        using PgLwLockExclusiveGuard<WorfTouchTable, long> guard = Counters.Exclusive();
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
            using PgLwLockExclusiveGuard<WorfTouchTable, long> guard = Counters.Exclusive();
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
        using PgLwLockShareGuard<WorfTouchTable, long> guard = Counters.Share();
        return (guard.Header.All, guard[Slot(seqoid)]);
    }

    private static void Count(PgLwLockExclusiveGuard<WorfTouchTable, long> guard, uint? seqoid)
    {
        if (seqoid is uint one)
        {
            int slot = Slot(one);
            guard[slot] = guard[slot] + 1;
        }
        else
        {
            WorfTouchTable header = guard.Header;
            header.All++;
            guard.Header = header;
        }
    }

    // The counter of a sequence of this database.
    private static int Slot(uint seqoid)
    {
        uint dboid;
        unsafe
        {
            dboid = NativeGlobals.MyDatabaseId;
        }

        return (int)(((dboid * 2654435761UL) ^ seqoid) % MaxCounters);
    }
}
