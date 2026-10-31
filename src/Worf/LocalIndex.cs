using Ankus.Postgres;

[assembly: PgSql("local_index", """
    CREATE TABLE worf.local_index (
        indexrelid oid NOT NULL PRIMARY KEY
    );

    COMMENT ON TABLE worf.local_index IS
        'Indexes that a failed concurrent build or reindex left on this node alone, whose DDL stays on this node';
    """)]

namespace Worf;

/// <summary>
/// Keeps track of the indexes on this node alone: those that a concurrent build whose DDL replicates, or a concurrent
/// reindex, left when it failed, which the other nodes never built. DDL on them stays on this node, however they're
/// rebuilt since, as by <c>REINDEX</c>, <c>VACUUM FULL</c>, or <c>ALTER TABLE</c>, which builds an index again under
/// its name.
/// </summary>
internal static unsafe partial class LocalIndex
{
    // Whether a concurrent build whose DDL replicates, or a concurrent reindex, runs now.
    private static bool s_tracking;
    private static bool s_reindexing;
    private static bool s_callbackRegistered;

    // The relations created while tracking, or while carrying a local index over, looked at once they're visible.
    private static readonly List<uint> s_created = [];

    // The indexes the running statement built, with their names then, which a reindex swaps with those they replace.
    private static readonly List<(uint Oid, string Name)> s_built = [];

    // The tables and names of the local indexes dropped to be built again, as ALTER TABLE drops an index to build it
    // again for a column's new type.
    private static readonly List<(uint Table, string Name)> s_rebuilding = [];

    [PgNativeCallback(nameof(TransactionEnding))]
    private static partial XactCallback TransactionCallback { get; }

    /// <summary>
    /// Starts tracking the indexes a statement builds, when it builds them concurrently and they may be left on this
    /// node alone: a concurrent build whose DDL replicates, or a concurrent reindex while DDL replicates.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <param name="replicated">Whether the statement is DDL to replicate.</param>
    /// <returns>Whether tracking started, for <see cref="EndStatement"/>.</returns>
    internal static bool BeginStatement(Node* parsetree, bool replicated)
    {
        bool reindexing = parsetree->type == NodeTag.T_ReindexStmt && Concurrently((ReindexStmt*)parsetree) && DdlReplication.Replicates();
        if (s_tracking || !(reindexing || (replicated && parsetree->type == NodeTag.T_IndexStmt && ((IndexStmt*)parsetree)->concurrent)))
        {
            return false;
        }

        if (!s_callbackRegistered)
        {
            NativeMethods.RegisterXactCallback(TransactionCallback, null);
            s_callbackRegistered = true;
        }

        s_built.Clear();
        s_tracking = true;
        s_reindexing = reindexing;
        return true;
    }

    /// <summary>
    /// Ends tracking the indexes a statement builds, which has run or failed.
    /// </summary>
    /// <param name="tracking">Whether <see cref="BeginStatement"/> started tracking.</param>
    internal static void EndStatement(bool tracking)
    {
        if (tracking)
        {
            s_tracking = false;
            s_reindexing = false;
        }
    }

    /// <summary>
    /// Notes that the concurrent build whose indexes are tracked queued its DDL, so that its index is on every node.
    /// </summary>
    internal static void Queued()
    {
        if (s_built.Count > 0)
        {
            Write("DELETE FROM worf.local_index WHERE indexrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[])",
                SpiParameter.Create(s_built.Select(built => (long)built.Oid).ToArray()));
            s_built.Clear();
        }
    }

    /// <summary>
    /// Notes a relation created, from the object access hook, while its catalog rows can't be read yet.
    /// </summary>
    /// <param name="relid">The relation.</param>
    internal static void NoteCreated(uint relid)
    {
        if (s_tracking || s_rebuilding.Count > 0)
        {
            s_created.Add(relid);
        }
    }

    /// <summary>
    /// Forgets a local index as it's dropped, from the object access hook, and notes one that PostgreSQL drops to
    /// build again, which it does under the same name in the same statement.
    /// </summary>
    /// <param name="relid">The relation.</param>
    /// <param name="internalDrop">Whether PostgreSQL drops it on its own, rather than as asked.</param>
    internal static void NoteDropped(uint relid, bool internalDrop)
    {
        if (NativeMethods.get_rel_relkind(relid) != (sbyte)'i' ||
            NativeMethods.get_rel_namespace(relid) == NativeMethods.get_namespace_oid(Worf.PStrDup(Worf.ExtensionName), true) || !Contains(relid))
        {
            return;
        }

        if (internalDrop)
        {
            s_rebuilding.Add((NativeMethods.IndexGetRelation(relid, false), Worf.FromServer(NativeMethods.get_rel_name(relid))));
        }

        Write("DELETE FROM worf.local_index WHERE indexrelid = $1", SpiParameter.Create(relid));
    }

    /// <summary>
    /// Checks whether an index is on this node alone.
    /// </summary>
    /// <param name="relid">The index.</param>
    /// <returns>Whether it is.</returns>
    internal static bool Contains(uint relid)
        => Worf.CatalogOid("local_index") != 0 &&
            Elevated.Run(() => Worf.ReadScalar<bool>("SELECT EXISTS (SELECT FROM worf.local_index WHERE indexrelid = $1)", SpiParameter.Create(relid)));

    /// <summary>
    /// Tracks the indexes created since the last call, once their catalog rows can be read: those a tracked statement
    /// built, and those built again under the name of a local index dropped to be. A statement calls this before it
    /// runs, and a transaction as it commits.
    /// </summary>
    internal static void Flush()
    {
        if (s_created.Count == 0 && s_rebuilding.Count == 0)
        {
            return;
        }

        List<uint> created = [.. s_created];
        List<(uint Table, string Name)> rebuilding = [.. s_rebuilding];
        s_created.Clear();
        s_rebuilding.Clear();
        if (NativeMethods.IsAbortedTransactionBlockState() || Worf.CatalogOid("local_index") == 0)
        {
            return;
        }

        List<uint> local = [];
        foreach (uint relid in created.Where(relid => NativeMethods.get_rel_relkind(relid) == (sbyte)'i'))
        {
            string name = Worf.FromServer(NativeMethods.get_rel_name(relid));
            if (s_tracking)
            {
                s_built.Add((relid, name));
                local.Add(relid);
            }
            else if (rebuilding.Contains((NativeMethods.IndexGetRelation(relid, false), name)))
            {
                local.Add(relid);
            }
        }

        Track(local);
    }

    private static void Track(List<uint> relids)
    {
        if (relids.Count > 0)
        {
            Write("INSERT INTO worf.local_index SELECT pg_catalog.unnest($1::pg_catalog.int8[])::pg_catalog.oid ON CONFLICT DO NOTHING",
                SpiParameter.Create(relids.Select(relid => (long)relid).ToArray()));
        }
    }

    /// <summary>
    /// Writes worf's own table, whoever runs the statement, with a snapshot, which a statement that commits on its way
    /// may lack between its steps.
    /// </summary>
    private static void Write(string command, params SpiParameter[] parameters)
        => _ = DdlReplication.WithSnapshot(() => Elevated.Run(() =>
        {
            _ = Spi.Execute(command, parameters);
            return true;
        }));

    /// <summary>
    /// Tracks the indexes created in a transaction as it commits, as a concurrent build or reindex commits each of its
    /// steps, and follows a concurrent reindex's swap.
    /// </summary>
    private static void TransactionEnding(XactEvent xactEvent, void* arg)
    {
        // A statement that commits on its way can't run in a transaction block, so it ends with any transaction that
        // aborts.
        if (xactEvent == XactEvent.XACT_EVENT_ABORT)
        {
            s_created.Clear();
            s_rebuilding.Clear();
            s_tracking = false;
            s_reindexing = false;
            return;
        }

        if (xactEvent == XactEvent.XACT_EVENT_PRE_COMMIT)
        {
            Flush();
            FollowSwaps();
        }
    }

    /// <summary>
    /// Follows a concurrent reindex's swap of the indexes it built with those they replace: each index built takes the
    /// name of the one it replaces, which, invalid under another name, is on this node alone until the reindex drops
    /// it, while the one built is on this node alone only if the one it replaces was.
    /// </summary>
    private static void FollowSwaps()
    {
        if (!s_reindexing || s_built.Count == 0)
        {
            return;
        }

        foreach ((uint relid, string name) in s_built.ToList())
        {
            sbyte* current = NativeMethods.get_rel_name(relid);
            if (current is null || Worf.FromServer(current) == name)
            {
                continue;
            }

            _ = s_built.Remove((relid, name));
            uint replaced = Elevated.Run(() => Worf.ReadScalar<uint?>("""
                SELECT o.indexrelid FROM pg_catalog.pg_index n
                    JOIN pg_catalog.pg_index o ON o.indrelid = n.indrelid AND o.indexrelid <> n.indexrelid
                WHERE n.indexrelid = $1 AND NOT o.indisvalid
                    AND o.xmin::text::bigint = pg_catalog.pg_current_xact_id_if_assigned()::text::bigint % 4294967296
                    AND o.indkey::text = n.indkey::text AND o.indclass::text = n.indclass::text
                    AND o.indcollation::text = n.indcollation::text AND o.indoption::text = n.indoption::text
                    AND pg_catalog.pg_get_expr(o.indexprs, o.indrelid) IS NOT DISTINCT FROM pg_catalog.pg_get_expr(n.indexprs, n.indrelid)
                    AND pg_catalog.pg_get_expr(o.indpred, o.indrelid) IS NOT DISTINCT FROM pg_catalog.pg_get_expr(n.indpred, n.indrelid)
                ORDER BY o.indexrelid LIMIT 1
                """, SpiParameter.Create(relid)) ?? 0);
            if (replaced == 0 || !Contains(replaced))
            {
                Write("DELETE FROM worf.local_index WHERE indexrelid = $1", SpiParameter.Create(relid));
            }

            if (replaced != 0)
            {
                Track([replaced]);
            }
        }
    }

    private static bool Concurrently(ReindexStmt* stmt)
    {
        for (int i = 0; stmt->@params is not null && i < stmt->@params->length; i++)
        {
            DefElem* option = (DefElem*)stmt->@params->elements[i].ptr_value;
            if (Worf.FromServer(option->defname) == "concurrently" && NativeMethods.defGetBoolean(option))
            {
                return true;
            }
        }

        return false;
    }
}
