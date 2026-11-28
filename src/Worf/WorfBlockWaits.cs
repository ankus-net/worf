namespace Worf;

/// <summary>
/// What worf's tables say of this node's neighbors, read once for a read of the blocks under the active snapshot, and
/// from it the node a draw from a sequence waits for. It gives what <see cref="SequenceRangeGuard.Unanswered"/>
/// gives as an expression of a query, worked out from the tables' rows, and changes with it.
/// </summary>
internal sealed class WorfBlockWaits
{
    private readonly long _started;
    private readonly HashSet<long> _complete;
    private readonly HashSet<uint> _dropped;
    private readonly List<(uint Origin, uint Target)> _subscriptions = [];
    private readonly HashSet<uint> _links = [];
    private readonly HashSet<uint> _named = [];
    private readonly HashSet<uint> _left = [];
    private readonly HashSet<uint> _nodes = [];
    private readonly List<(uint Seqoid, uint Node, byte[] Name, uint Via, long? Confirmed)> _reports = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="WorfBlockWaits"/> class from worf's tables.
    /// </summary>
    /// <param name="started">This server's start.</param>
    /// <param name="complete">The neighbors whose messages were taken in whole since the start.</param>
    /// <param name="dropped">The neighbors this transaction dropped.</param>
    internal WorfBlockWaits(long started, long[] complete, uint[] dropped)
    {
        _started = started;
        _complete = [.. complete];
        _dropped = [.. dropped];
        foreach (object?[] row in Worf.ScanCatalog("subscription", "sub_origin", "sub_target"))
        {
            _subscriptions.Add(((uint)row[0]!, (uint)row[1]!));
        }

        foreach (object?[] row in Worf.ScanCatalog("sequence_range_link", "node_id"))
        {
            _ = _links.Add((uint)row[0]!);
        }

        foreach (object?[] row in Worf.ScanCatalog("sequence_range_named", "neighbor"))
        {
            _ = _named.Add((uint)row[0]!);
        }

        foreach (object?[] row in Worf.ScanCatalog("sequence_range_left", "neighbor"))
        {
            _ = _left.Add((uint)row[0]!);
        }

        foreach (object?[] row in Worf.ScanCatalog("node", "node_id"))
        {
            _ = _nodes.Add((uint)row[0]!);
        }

        foreach (object?[] row in Worf.ScanCatalog("sequence_range_node", "seqoid", "node_id", "node_name", "via_node", "confirmed_start"))
        {
            _reports.Add(((uint)row[0]!, (uint)row[1]!, (byte[])row[2]!, (uint)row[3]!, (long?)row[4]));
        }
    }

    /// <summary>
    /// Gets the node a draw from a sequence's active block waits for, or null for none: the neighbor that registers
    /// this node's other neighbors for a block another node granted since this server's start, and otherwise the
    /// node that hasn't answered since the start.
    /// </summary>
    /// <param name="self">This node.</param>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="parent">The sequence's parent node, or null.</param>
    /// <param name="from">The node the block came from, or null.</param>
    /// <param name="receivedAt">When the block came.</param>
    /// <returns>The node, or null.</returns>
    internal uint? WaitFor(uint self, uint seqoid, uint? parent, uint? from, long receivedAt)
        => from is uint granted && granted != self && receivedAt > _started ? Registering(self, parent) : Unanswered(self, seqoid, parent, from);

    // The nodes this node subscribes to and those that call it, in their order.
    private SortedSet<uint> Neighbors(uint self) => [.. _subscriptions.Where(subscription => subscription.Target == self).Select(subscription => subscription.Origin), .. _links];

    // The neighbor a draw waits for while another neighbor isn't yet one that a neighbor says this node has: one
    // that exchanges with this node, the sequence's parent first.
    private uint? Registering(uint self, uint? parent)
    {
        SortedSet<uint> neighbors = Neighbors(self);
        uint? first = null;
        foreach (uint node in neighbors)
        {
            if (node == self || !_reports.Exists(report => report.Node == node && report.Via == node) ||
                !neighbors.Any(other => other != node && other != self && !_named.Contains(other)))
            {
                continue;
            }

            if (node == parent)
            {
                return node;
            }

            first ??= node;
        }

        return first;
    }

    // The node that hasn't answered since this server's start: the one that granted the block, a neighbor that
    // reports the sequence, by its name, a neighbor that reports nothing of it, by its number, and then the one
    // that registers the others.
    private uint? Unanswered(uint self, uint seqoid, uint? parent, uint? grantor)
    {
        if (grantor is uint granted && granted != self && (_nodes.Contains(granted) || _links.Contains(granted)) &&
            !_reports.Exists(report => report.Seqoid == seqoid && report.Node == granted && report.Confirmed == _started))
        {
            return granted;
        }

        (uint Node, byte[] Name)? reporting = null;
        foreach ((uint sequence, uint node, byte[] name, _, long? confirmed) in _reports)
        {
            if (sequence != seqoid || node == self || confirmed == _started)
            {
                continue;
            }

            bool neighbor = _subscriptions.Exists(subscription => subscription.Origin == node && subscription.Target == self) || _links.Contains(node) ||
                (_named.Contains(node) && !_left.Contains(node)) || _dropped.Contains(node);
            if (neighbor && (reporting is not (uint, byte[]) least || name.AsSpan().SequenceCompareTo(least.Item2) < 0))
            {
                reporting = (node, name);
            }
        }

        if (reporting is (uint unanswered, byte[]))
        {
            return unanswered;
        }

        SortedSet<uint> known = Neighbors(self);
        known.UnionWith(_named.Where(node => !_left.Contains(node)));
        known.UnionWith(_dropped);
        foreach (uint node in known)
        {
            if (node != self && !_complete.Contains(node) && !_reports.Exists(report => report.Seqoid == seqoid && report.Node == node))
            {
                return node;
            }
        }

        return Registering(self, parent);
    }
}
