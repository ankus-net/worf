using Ankus.Postgres;

namespace Worf;

/// <summary>
/// When this server last heard from each node of a database's sequence ranges, in shared memory, so that an exchange
/// that brings no change writes nothing and an idle node uses no transaction IDs. The tables record a node's report when
/// it changes. Each query that asks how recently a node reported takes the later of the two, and a node that this server
/// hasn't heard from since it started counts, for the checks that wait on it, as heard when it started. A node the table
/// has no room for has its reports written to the tables instead.
/// </summary>
internal static class SequenceRangeHeard
{
    /// <summary>
    /// The most nodes the table holds, for every database of the server.
    /// </summary>
    internal const int MaxNodes = 1024;

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
    /// <param name="direct">Whether the node exchanged with this server, rather than a neighbor relaying its report.</param>
    /// <param name="at">The time, in microseconds since 2000.</param>
    /// <returns>Whether the table recorded it, which it doesn't when it has no room for the node.</returns>
    internal static bool Record(uint nodeId, bool direct, long at)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId);
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
        int slot = Find(guard, nodeId);
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
    /// heard from through it for a day. An entry a node is heard from through is never given up, so that reports of
    /// other nodes, however many, can't make the node look silent, or drop the WAL position it was last at.
    /// </summary>
    /// <returns>The entry, or -1 when the table has no room.</returns>
    private static int Find(PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard, uint nodeId)
    {
        uint dboid = DatabaseId();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                return i;
            }
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

        if (slot >= 0)
        {
            guard[slot] = new WorfHeard { Dboid = dboid, NodeId = nodeId };
        }

        return slot;
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

    private static uint DatabaseId()
    {
        unsafe
        {
            return NativeGlobals.MyDatabaseId;
        }
    }
}
