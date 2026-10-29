using System.Globalization;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Range management for a new subscription's sync. The provider's managed sequences are quarantined in the transaction
/// that creates them when the structure is restored, and joined before any rows are copied, so the copied rows, which
/// carry other nodes' values, never count against this node's high-water mark.
/// </summary>
internal static class SequenceRangeSync
{
    /// <summary>
    /// Lists the sequences the provider manages.
    /// </summary>
    /// <param name="dsn">The provider's connection string.</param>
    /// <param name="name">The subscription's name.</param>
    /// <returns>Each sequence's qualified name, owner, epoch, sizes and root span.</returns>
    internal static List<WorfManagedSequence> FetchManaged(string dsn, string name)
    {
        List<WorfManagedSequence> managed = [];
        using PgReplicationConnection conn = Worf.Connect(dsn, name, "ranges");
        if (conn.Query("SELECT to_regclass('worf.sequence_range') IS NOT NULL", 1)[0][0] != "t")
        {
            return managed;
        }

        foreach (string?[] row in conn.Query("""
            SELECT pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname), r.owner_node, r.epoch, r.range_size,
                r.max_range_size, r.root_start, r.root_end
            FROM worf.sequence_range r
                JOIN pg_catalog.pg_class c ON c.oid = r.seqoid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE r.state NOT IN ('disabled', 'refused')
            """, 7))
        {
            managed.Add(new WorfManagedSequence(row[0]!, uint.Parse(row[1]!, CultureInfo.InvariantCulture), int.Parse(row[2]!, CultureInfo.InvariantCulture),
                long.Parse(row[3]!, CultureInfo.InvariantCulture), long.Parse(row[4]!, CultureInfo.InvariantCulture),
                row[5] is null ? null : long.Parse(row[5]!, CultureInfo.InvariantCulture),
                row[6] is null ? null : long.Parse(row[6]!, CultureInfo.InvariantCulture)));
        }

        return managed;
    }

    /// <summary>
    /// Records the provider's managed sequences for the session that restores its structure.
    /// </summary>
    /// <param name="managed">The sequences.</param>
    internal static void RecordForRestore(List<WorfManagedSequence> managed)
    {
        _ = Spi.Execute("DELETE FROM worf.sequence_range_restore");
        foreach (WorfManagedSequence sequence in managed)
        {
            _ = Spi.Execute("INSERT INTO worf.sequence_range_restore VALUES ($1, $2, $3, $4, $5, $6, $7)", SpiParameter.Create(sequence.Name),
                SpiParameter.Create(sequence.Owner), SpiParameter.Create(sequence.Epoch), SpiParameter.Create(sequence.RangeSize),
                SpiParameter.Create(sequence.MaxRangeSize), SpiParameter.Create(sequence.RootStart), SpiParameter.Create(sequence.RootEnd));
        }
    }

    /// <summary>
    /// Forgets the provider's managed sequences once the structure is restored.
    /// </summary>
    internal static void ClearRestore() => _ = Spi.Execute("DELETE FROM worf.sequence_range_restore");

    /// <summary>
    /// Joins this node to the provider's managed sequences that exist here, before rows are copied into the tables
    /// that use them.
    /// </summary>
    /// <param name="managed">The provider's managed sequences.</param>
    /// <param name="relid">The one table about to be copied, or zero for every table.</param>
    internal static void JoinBeforeCopy(List<WorfManagedSequence> managed, uint relid)
    {
        if (managed.Count == 0 || WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
        {
            return;
        }

        foreach (WorfManagedSequence sequence in managed)
        {
            if (Spi.ExecuteScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(sequence.Name)) is uint seqoid &&
                !SequenceRanges.IsManaged(seqoid) && (relid == 0 || SequenceRanges.UsingColumns(seqoid).Exists(column => column.Relid == relid)))
            {
                SequenceRanges.JoinFromNeighbor(seqoid, sequence.Owner, sequence.Epoch, sequence.RangeSize, sequence.MaxRangeSize, sequence.RootStart,
                    sequence.RootEnd, node);
            }
        }

        SequenceRanges.ChangedOnCommit();
    }

    /// <summary>
    /// Checks whether this session restores a subscription's structure.
    /// </summary>
    /// <returns>Whether it does.</returns>
    internal static unsafe bool Restoring()
    {
        fixed (byte* name = Worf.ToServerCString("worf.subscription_schema_restore"))
        {
            sbyte* value = NativeMethods.GetConfigOption((sbyte*)name, true, false);
            return value is not null && Worf.FromServer(value) is "true" or "on" or "1";
        }
    }

    /// <summary>
    /// Quarantines the provider's managed sequences that the restore has created so far, in the transaction that
    /// created them, and joins them.
    /// </summary>
    internal static void JoinRestored()
    {
        if (WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
        {
            return;
        }

        SpiResult listed = Worf.SelectCatalog(
            "SELECT name, owner_node, epoch, range_size, max_range_size, root_start, root_end FROM worf.sequence_range_restore");
        for (int i = 0; i < listed.Count; i++)
        {
            SpiRow row = listed[i];
            if (Spi.ExecuteScalar<uint?>("SELECT to_regclass($1)::oid", SpiParameter.Create(row.Get<string>(0))) is uint seqoid &&
                !SequenceRanges.IsManaged(seqoid))
            {
                SequenceRanges.JoinFromNeighbor(seqoid, row.Get<uint>(1), row.Get<int>(2), row.Get<long>(3), row.Get<long>(4), row.Get<long?>(5),
                    row.Get<long?>(6), node);
            }
        }
    }
}
