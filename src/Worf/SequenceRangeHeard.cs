using Ankus.Postgres;

namespace Worf;

/// <summary>
/// When this server last heard from each node of a database's sequence ranges, in shared memory, so that an exchange
/// that brings no change writes nothing and an idle node uses no transaction IDs. The tables record a node's report when
/// it changes. Each query that asks how recently a node reported takes the later of the two, and a node that this server
/// hasn't heard from since it started counts, for the checks that wait on it, as heard when it started. A node the table
/// has no room for has its reports written to the tables instead. Each neighbor's relayed reports claim a quarter of the
/// table at most, so that made-up reports can't fill it, and a node heard from directly takes the entry of one only ever
/// relayed when it finds no other.
/// </summary>
internal static class SequenceRangeHeard
{
    /// <summary>
    /// The most nodes the table holds, for every database of the server.
    /// </summary>
    internal const int MaxNodes = 1024;

    // The most entries the reports one neighbor relays claim.
    internal const int MaxRelayed = MaxNodes / 4;

    // How long an entry stays once its node is no longer heard from: as long as anything waits on hearing from a node,
    // the day after which a Snowflake node number a node stopped reporting comes back.
    private const long KeepFor = 86_400_000_000;

    /// <summary>
    /// Gets the shared table of when this server last heard from each node.
    /// </summary>
    internal static PgLwLock<WorfHeardTable, WorfHeard> Nodes { get; } = new("worf_sequence_range_heard");

    /// <summary>
    /// Sets up the shared memory.
    /// </summary>
    internal static void ShmemInit()
        => PgSharedMemory.Initialize(Nodes, MaxNodes, static () => new WorfHeardTable { Since = Worker.GetCurrentTimestamp() }, static _ => default);

    /// <summary>
    /// Records a report from a node of this database, made at a time.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="via">The node that exchanged with this server, itself unless it relayed the report.</param>
    /// <param name="at">The time, in microseconds since 2000.</param>
    /// <returns>Whether the table recorded it, which it doesn't when it has no room for the node.</returns>
    internal static bool Record(uint nodeId, uint via, long at)
    {
        bool direct = via == nodeId;
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId, via);
        if (slot < 0)
        {
            return false;
        }

        WorfHeard heard = guard[slot];
        heard.ReportedAt = Math.Max(heard.ReportedAt, at);
        if (direct)
        {
            heard.DirectAt = Math.Max(heard.DirectAt, at);
        }

        guard[slot] = heard;
        return true;
    }

    /// <summary>
    /// Records the WAL position a neighbor's call or answer carried, which this server compares the next one with to see
    /// the neighbor go back. The tables record it only when it moved by a WAL segment or more, since a node that writes
    /// nothing else still writes checkpoints and snapshots of its running transactions.
    /// </summary>
    /// <param name="nodeId">The neighbor.</param>
    /// <param name="sysid">The neighbor's system identifier.</param>
    /// <param name="timeline">The neighbor's timeline.</param>
    /// <param name="call">Whether the neighbor called this server, rather than answered its call.</param>
    /// <param name="lsn">The position.</param>
    internal static void RecordLsn(uint nodeId, ulong sysid, long timeline, bool call, ulong lsn)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId, nodeId);
        if (slot < 0)
        {
            return;
        }

        WorfHeard heard = guard[slot];
        if (heard.Sysid != sysid || heard.Timeline != timeline)
        {
            (heard.Sysid, heard.Timeline, heard.CallLsn, heard.ReplyLsn) = (sysid, timeline, 0, 0);
        }

        if (call)
        {
            heard.CallLsn = lsn;
        }
        else
        {
            heard.ReplyLsn = lsn;
        }

        guard[slot] = heard;
    }

    /// <summary>
    /// Gets the WAL position a neighbor's last call or answer carried, on a system and timeline.
    /// </summary>
    /// <param name="nodeId">The neighbor.</param>
    /// <param name="sysid">The system identifier.</param>
    /// <param name="timeline">The timeline.</param>
    /// <param name="call">Whether to get the position of its calls, rather than its answers.</param>
    /// <returns>The position, or zero.</returns>
    internal static ulong LatestLsn(uint nodeId, ulong sysid, long timeline, bool call)
    {
        uint dboid = DatabaseId();
        using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId && guard[i].Sysid == sysid && guard[i].Timeline == timeline)
            {
                return call ? guard[i].CallLsn : guard[i].ReplyLsn;
            }
        }

        return 0;
    }

    /// <summary>
    /// Finds a node's entry, or claims one: a free one, or else the one heard from least recently, if no node has been
    /// heard from through it for a day. A node heard from directly that finds none takes the entry of the node heard
    /// from least recently of those only ever relayed. An entry a node is heard from through is otherwise never given
    /// up, so that reports of other nodes, however many, can't make the node look silent, or drop the WAL position it
    /// was last at, and a neighbor's relayed reports claim no more than their share.
    /// </summary>
    /// <returns>The entry, or -1 when the table has no room.</returns>
    private static int Find(PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard, uint nodeId, uint via)
    {
        uint dboid = DatabaseId();
        int relayed = 0;
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                return i;
            }

            if (guard[i].Dboid == dboid && guard[i].DirectAt == 0 && guard[i].Via == via)
            {
                relayed++;
            }
        }

        bool direct = via == nodeId;
        if (!direct && relayed >= MaxRelayed)
        {
            return -1;
        }

        long forgotten = Worker.GetCurrentTimestamp() - KeepFor;
        int slot = -1;
        for (int i = 0; i < guard.Length && (slot < 0 || guard[slot].Dboid != 0); i++)
        {
            long heard = Math.Max(guard[i].ReportedAt, guard[i].DirectAt);
            if (guard[i].Dboid == 0 || (heard < forgotten && (slot < 0 || heard < Math.Max(guard[slot].ReportedAt, guard[slot].DirectAt))))
            {
                slot = i;
            }
        }

        if (direct && slot < 0)
        {
            for (int i = 0; i < guard.Length; i++)
            {
                if (guard[i].DirectAt == 0 && (slot < 0 || guard[i].ReportedAt < guard[slot].ReportedAt))
                {
                    slot = i;
                }
            }
        }

        if (slot >= 0)
        {
            guard[slot] = new WorfHeard { Dboid = dboid, NodeId = nodeId, Via = direct ? 0 : via };
        }

        return slot;
    }

    /// <summary>
    /// Gets when this server started, as its messages tell its neighbors, which they say back once they've heard it.
    /// </summary>
    internal static long Started
    {
        get
        {
            using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
            return guard.Header.Since;
        }
    }

    /// <summary>
    /// Gets the WAL position this server started from, as the checkpoint it started from, or the one that ended its
    /// recovery, gives it, read once, or <see cref="ulong.MaxValue"/> when a later checkpoint took that one's place before
    /// it was read. A neighbor that heard of a later position from this node before it started knows from it that the
    /// node went back.
    /// </summary>
    internal static ulong StartLsn
    {
        get
        {
            using (PgLwLockShareGuard<WorfHeardTable, WorfHeard> shared = Nodes.Share())
            {
                if (shared.Header.StartLsn != 0)
                {
                    return shared.Header.StartLsn;
                }
            }

            WorfRows checkpoint = Worf.Read("SELECT redo_lsn::text, checkpoint_time <= $1 FROM pg_catalog.pg_control_checkpoint()", 1,
                SpiParameter.Create(new PgTimestampTz(Started + 60_000_000L)));
            ulong lsn = checkpoint.Count > 0 && checkpoint[0].Get<bool>(1) && Worf.TryParseLsn(checkpoint[0].Get<string?>(0) ?? "", out ulong redo)
                ? redo : ulong.MaxValue;
            using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
            if (guard.Header.StartLsn == 0)
            {
                WorfHeardTable header = guard.Header;
                header.StartLsn = lsn;
                guard.Header = header;
            }

            return guard.Header.StartLsn;
        }
    }

    /// <summary>
    /// Records when a node's server started, as its message says.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="startedAt">When its server started.</param>
    internal static void RecordStarted(uint nodeId, long startedAt)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId, nodeId);
        if (slot >= 0)
        {
            WorfHeard heard = guard[slot];
            heard.StartedAt = startedAt;
            guard[slot] = heard;
        }
    }

    /// <summary>
    /// Counts a notice a node sent one of this server's processes, in the minute that began last, for every process.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="now">The time.</param>
    /// <returns>How many notices the node sent in the minute before this one, or -1 when shared memory has no room for
    /// the node.</returns>
    internal static int CountNotice(uint nodeId, long now)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId, nodeId);
        if (slot < 0)
        {
            return -1;
        }

        WorfHeard heard = guard[slot];
        if (now - heard.NoticeMinute >= 60_000_000L)
        {
            (heard.NoticeMinute, heard.Notices) = (now, 0);
        }

        int before = heard.Notices;
        heard.Notices++;
        guard[slot] = heard;
        return before;
    }

    /// <summary>
    /// Gets where, among the sequences of a node's calls, the last call this server cut short stopped, or zero.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <returns>The position.</returns>
    internal static int ResumeAt(uint nodeId)
    {
        uint dboid = DatabaseId();
        using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                return guard[i].ResumeAt;
            }
        }

        return 0;
    }

    /// <summary>
    /// Notes where, among the sequences of a node's call, the call was cut short, or zero for one that wasn't.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="position">The position.</param>
    internal static void RecordResumeAt(uint nodeId, int position)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId, nodeId);
        if (slot >= 0)
        {
            WorfHeard heard = guard[slot];
            heard.ResumeAt = position;
            guard[slot] = heard;
        }
    }

    /// <summary>
    /// Gets when a node's server started, as its last message said, or zero.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <returns>The time.</returns>
    internal static long StartedOf(uint nodeId)
    {
        uint dboid = DatabaseId();
        using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                return guard[i].StartedAt;
            }
        }

        return 0;
    }

    /// <summary>
    /// Notes, as a transaction that took in a node's message commits, a message the node made once it had heard of this
    /// server's start, that this server has heard from it since it started, and done what the message asked, as a reset.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    internal static void Confirm(uint nodeId)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId, nodeId);
        if (slot >= 0)
        {
            WorfHeard heard = guard[slot];
            heard.ConfirmedAt = Worker.GetCurrentTimestamp();
            guard[slot] = heard;
        }
    }

    /// <summary>
    /// Notes that a statement waits for this server to hear from a node, its grantor, since it started, which makes the
    /// exchange with the node due.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    internal static void Want(uint nodeId)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId, nodeId);
        if (slot >= 0)
        {
            WorfHeard heard = guard[slot];
            heard.WantedAt = Worker.GetCurrentTimestamp();
            guard[slot] = heard;
        }
    }

    /// <summary>
    /// Notes that a node, this server's provider, asked this server to call it, which makes the exchange with it due.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    internal static void CallFor(uint nodeId)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId, nodeId);
        if (slot >= 0)
        {
            WorfHeard heard = guard[slot];
            heard.CallForAt = Worker.GetCurrentTimestamp();
            guard[slot] = heard;
        }
    }

    /// <summary>
    /// Checks whether a node asked this server to call it since a time.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="since">The time.</param>
    /// <returns>Whether it did.</returns>
    internal static bool CalledFor(uint nodeId, long since)
    {
        uint dboid = DatabaseId();
        using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                return guard[i].CallForAt >= since;
            }
        }

        return false;
    }

    /// <summary>
    /// Checks whether a statement waits for this server to hear from a node since it started, as it did since a time,
    /// while it hasn't yet.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <param name="since">The time.</param>
    /// <returns>Whether one does.</returns>
    internal static bool Wanted(uint nodeId, long since)
    {
        uint dboid = DatabaseId();
        using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                return guard[i].ConfirmedAt == 0 && guard[i].WantedAt >= since;
            }
        }

        return false;
    }

    /// <summary>
    /// Checks whether this server has taken in a node's own message, and committed it, since it started.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <returns>Whether it has.</returns>
    internal static bool Confirmed(uint nodeId)
    {
        uint dboid = DatabaseId();
        using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                return guard[i].ConfirmedAt != 0;
            }
        }

        return false;
    }

    /// <summary>
    /// Checks whether the table holds a node's entry.
    /// </summary>
    /// <param name="nodeId">The node.</param>
    /// <returns>Whether it does.</returns>
    internal static bool Holds(uint nodeId)
    {
        uint dboid = DatabaseId();
        using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns when this server last heard from a node: the later of when its tables recorded and of what it heard since
    /// it started.
    /// </summary>
    /// <param name="node">The node.</param>
    /// <param name="direct">Whether to count only the node's own exchanges with this server, as for a neighbor.</param>
    /// <param name="recorded">When the tables recorded the node's report.</param>
    /// <param name="assumeHeard">Whether a node not heard from since this server started counts as heard when it
    /// started, for a check that must not go ahead without it.</param>
    /// <returns>The time.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_heard(node oid, direct boolean, recorded timestamptz, assume_heard boolean)
        RETURNS timestamptz STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        REVOKE ALL ON FUNCTION worf.sequence_range_heard(oid, boolean, timestamptz, boolean) FROM public;
        """)]
    internal static PgTimestampTz SequenceRangeHeardAt(uint node, bool direct, PgTimestampTz recorded, bool assumeHeard)
    {
        uint dboid = DatabaseId();
        long at = recorded.MicrosecondsSinceEpoch;
        using PgLwLockShareGuard<WorfHeardTable, WorfHeard> guard = Nodes.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == node)
            {
                return new PgTimestampTz(Math.Max(at, direct ? guard[i].DirectAt : guard[i].ReportedAt));
            }
        }

        return new PgTimestampTz(assumeHeard ? Math.Max(at, guard.Header.Since) : at);
    }

    /// <summary>
    /// Checks whether a neighbor may have this server reset its sequences' blocks now: once every ten minutes, with the
    /// resets of the minute after it, which one rollback brings for every sequence, each sequence once, and twice as
    /// long after each reset that comes within a day of the last window's end, up to a day, and half as long for each two
    /// days and a window more in which none came, so that messages made up to reset it, which may fence an owner's root
    /// until every participant reports, do so ever more seldom, however they pause, while a rollback, which comes seldom,
    /// resets it at once. A neighbor that saw this node roll back keeps asking meanwhile. This server starts with none, as
    /// after a restore, which takes the first at once.
    /// </summary>
    /// <param name="nodeId">The neighbor.</param>
    /// <returns>When the ten minutes whose resets the neighbor may have made began, so that a sequence it has reset since
    /// then isn't reset again, or null when it may make none now.</returns>
    /// <param name="lift">Whether the request is the neighbor's, as this server's grantor, from a fence's lift that names
    /// blocks it granted this server, which it may make in a window of its own, so that another reset doesn't hold it
    /// up, ten seconds at first rather than ten minutes, as a fence the grantor forced may come soon after another, and
    /// growing as the other window does, so that made up ones reset this server ever more seldom.</param>
    internal static long? MayReset(uint nodeId, bool lift = false)
    {
        uint dboid = DatabaseId();
        long now = Worker.GetCurrentTimestamp();
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid != dboid || guard[i].NodeId != nodeId)
            {
                continue;
            }

            WorfHeard heard = guard[i];
            long? window;
            if (lift)
            {
                (window, heard.LiftResetAt, heard.LiftResetEvery) = ResetWindow(heard.LiftResetAt, heard.LiftResetEvery, now, 10_000_000L);
            }
            else
            {
                (window, heard.ResetAt, heard.ResetEvery) = ResetWindow(heard.ResetAt, heard.ResetEvery, now);
            }

            guard[i] = heard;
            return window;
        }

        return now;
    }

    /// <summary>
    /// Works out whether a neighbor may have this server reset now, as <see cref="MayReset"/> tells, from when its
    /// resets' window last began and how long it lasts.
    /// </summary>
    /// <param name="resetAt">When the window last began, or zero for none yet.</param>
    /// <param name="resetEvery">How long it lasts, in microseconds, or zero for ten minutes.</param>
    /// <param name="now">The time now.</param>
    /// <returns>When the window the neighbor may reset in began, or null when it may not now, and when the window
    /// begins and how long it lasts from now on.</returns>
    /// <param name="shortest">The window's shortest, in microseconds: ten minutes, or ten seconds for a grantor's lifts.</param>
    internal static (long? Window, long ResetAt, long ResetEvery) ResetWindow(long resetAt, long resetEvery, long now, long shortest = 600_000_000L)
    {
        long every = Math.Max(resetEvery, shortest);
        if (resetAt != 0 && now - resetAt >= 60_000_000 && now - resetAt < every)
        {
            return (null, resetAt, resetEvery);
        }

        if (resetAt == 0)
        {
            return (now, now, shortest);
        }

        if (now - resetAt >= every)
        {
            // The window doubles after a reset within a day of its end, and halves for each two days and a window more
            // in which the neighbor could have reset this server and didn't, which would have let a neighbor that kept
            // asking reset it at least twice, so one that pauses resets it no more often than one that keeps asking.
            long quiet = now - resetAt - every;
            return (now, now, quiet < 86_400_000_000L
                ? Math.Min(every * 2, 86_400_000_000L)
                : Math.Max(every >> (int)Math.Min(quiet / (172_800_000_000L + every), 62), shortest));
        }

        return (resetAt, resetAt, resetEvery);
    }

    private static uint DatabaseId()
    {
        unsafe
        {
            return NativeGlobals.MyDatabaseId;
        }
    }
}
