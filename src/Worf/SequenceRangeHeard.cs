using Ankus.Postgres;

namespace Worf;

/// <summary>
/// When this server last heard from each node of a database's sequence ranges, in shared memory, so that an exchange
/// that brings no change writes nothing and an idle node uses no transaction IDs. The tables record a node's report when
/// it changes. Each query that asks how recently a node reported takes the later of the two, and a node that this server
/// hasn't heard from since it started counts, for the checks that wait on it, as heard when it started.
/// </summary>
internal static class SequenceRangeHeard
{
    private const int MaxNodes = 1024;

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
    internal static void Record(uint nodeId, bool direct, long at)
    {
        using PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard = Nodes.Exclusive();
        int slot = Find(guard, nodeId);
        WorfHeard heard = guard[slot];
        heard.ReportedAt = Math.Max(heard.ReportedAt, at);
        if (direct)
        {
            heard.DirectAt = Math.Max(heard.DirectAt, at);
        }

        guard[slot] = heard;
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
    /// Finds a node's entry, or claims one.
    /// </summary>
    private static int Find(PgLwLockExclusiveGuard<WorfHeardTable, WorfHeard> guard, uint nodeId)
    {
        uint dboid = DatabaseId();
        int slot = -1;
        for (int i = 0; i < guard.Length && slot < 0; i++)
        {
            if (guard[i].Dboid == dboid && guard[i].NodeId == nodeId)
            {
                slot = i;
            }
        }

        // A full table gives up the entry heard from least recently, which then counts as heard when the table was set up.
        if (slot < 0)
        {
            for (int i = 0; i < guard.Length; i++)
            {
                if (slot < 0 || guard[i].Dboid == 0 || (guard[slot].Dboid != 0 && guard[i].ReportedAt < guard[slot].ReportedAt))
                {
                    slot = i;
                }
            }

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
