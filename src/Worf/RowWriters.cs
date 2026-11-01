using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Tells whether a subscription wrote rows of a table, from their commit origins and what worf recorded the origins
/// applied for, as a table sync's merge needs. The partitions it reads stay open, without giving up their locks, until
/// it's disposed.
/// </summary>
/// <param name="subId">The subscription.</param>
internal sealed unsafe class RowWriters(uint subId) : IDisposable
{
    private const int NoLock = 0;
    private const int AccessShareLock = 1;

    private readonly Dictionary<uint, nint> _relations = [];
    private readonly Dictionary<(ushort Origin, uint Xid), bool> _written = [];
    private readonly bool _tracked = NativeGlobals.track_commit_timestamp;
    private readonly ulong _keptSince = WorfNode.OriginWriterKeptSince;

    /// <summary>
    /// Tells whether the subscription wrote a row: true when its commit's origin applied for the subscription then,
    /// false when it came from elsewhere, as when it was written here or under an origin that applied for something
    /// else, and null when that can't be told: commit timestamps aren't tracked or are gone, the row is frozen, or what
    /// worf recorded for its origin may be forgotten.
    /// </summary>
    /// <param name="relid">The row's table or partition.</param>
    /// <param name="block">The row's block.</param>
    /// <param name="offset">The row's offset in its block.</param>
    /// <param name="xid">The row's transaction.</param>
    /// <returns>Whether the subscription wrote it, or null.</returns>
    public bool? WrittenBy(uint relid, uint block, ushort offset, uint xid)
    {
        if (!_tracked || IsFrozen(Relation(relid), block, offset))
        {
            return null;
        }

        long timestamp;
        ushort origin;
        if (!NativeMethods.TransactionIdGetCommitTsData(xid, &timestamp, &origin))
        {
            return null;
        }

        if (origin == 0)
        {
            return false;
        }

        if (Worf.FullTransactionId(xid) < _keptSince)
        {
            return null;
        }

        if (!_written.TryGetValue((origin, xid), out bool written))
        {
            written = WorfNode.OriginWriter(origin, xid, recordedOnly: true)?.SubId == subId;
            _written.Add((origin, xid), written);
        }

        return written;
    }

    /// <summary>
    /// Closes the partitions it read, keeping their locks until the transaction ends.
    /// </summary>
    public void Dispose()
    {
        foreach (nint relation in _relations.Values)
        {
            NativeMethods.relation_close((RelationData*)relation, NoLock);
        }

        _relations.Clear();
    }

    /// <summary>
    /// Checks whether the row at a position is frozen, as its tuple header says.
    /// </summary>
    private static bool IsFrozen(RelationData* relation, uint block, ushort offset)
    {
        HeapTupleData tuple = default;
        tuple.t_self.ip_blkid.bi_hi = (ushort)(block >> 16);
        tuple.t_self.ip_blkid.bi_lo = (ushort)block;
        tuple.t_self.ip_posid = offset;
        int buffer;
        if (!NativeMethods.heap_fetch(relation, NativeMethods.GetLatestSnapshot(), &tuple, &buffer, false))
        {
            return false;
        }

        bool frozen = ApplyHeap.XminFrozen(tuple.t_data);
        NativeMethods.ReleaseBuffer(buffer);
        return frozen;
    }

    private RelationData* Relation(uint relid)
    {
        if (!_relations.TryGetValue(relid, out nint relation))
        {
            relation = (nint)NativeMethods.relation_open(relid, AccessShareLock);
            _relations.Add(relid, relation);
        }

        return (RelationData*)relation;
    }
}
