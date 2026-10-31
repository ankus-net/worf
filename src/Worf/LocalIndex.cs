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

    // The indexes of the tables a concurrent reindex builds indexes for, by their names before it swaps them, as each
    // index built takes the name of the one it replaces.
    private static readonly Dictionary<(uint Table, string Name), uint> s_namedBefore = [];

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

        RegisterCallback();
        s_built.Clear();
        s_namedBefore.Clear();
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

        // The index built again is tracked as the transaction commits, if not at the transaction's next statement, so
        // that other sessions find it on this node alone too.
        if (internalDrop)
        {
            RegisterCallback();
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

        if (s_reindexing && local.Count > 0)
        {
            NoteNames([.. local.Select(relid => NativeMethods.IndexGetRelation(relid, false)).Distinct()]);
        }

        Track(local);
    }

    /// <summary>
    /// Notes the names of the indexes of tables that a concurrent reindex builds indexes for, before it swaps them.
    /// </summary>
    private static void NoteNames(List<uint> tables)
    {
        SpiResult result = Worf.Read("""
            SELECT i.indrelid, i.indexrelid, c.relname::pg_catalog.text FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
            WHERE i.indrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[])
            """, 0, SpiParameter.Create(tables.Select(table => (long)table).ToArray()));
        foreach (SpiRow row in result)
        {
            s_namedBefore[(row.Get<uint>(0), row.Get<string>(2))] = row.Get<uint>(1);
        }
    }

    /// <summary>
    /// Reads how the indexes on this node alone are tied to other objects, before DDL that replicates and may tie one
    /// to them: as a partition of another index, its table's replica identity or clustering index, or by what depends
    /// on it, or it on other than its table's columns, as a constraint that it becomes the index of, or a foreign key
    /// that references its table by it.
    /// </summary>
    /// <param name="parsetree">The DDL.</param>
    /// <returns>The ties, for <see cref="CheckTies"/>, or null when the DDL can't tie any.</returns>
    internal static Dictionary<uint, (string Name, bool Partition, bool Identity, bool Clustered, long Dependencies)>? Ties(Node* parsetree)
        => (parsetree->type is NodeTag.T_AlterTableStmt or NodeTag.T_CreateStmt ||
                (parsetree->type == NodeTag.T_IndexStmt && !((IndexStmt*)parsetree)->concurrent)) && Worf.CatalogOid("local_index") != 0
            ? ReadTies()
            : null;

    /// <summary>
    /// Fails DDL that replicates and has tied an index on this node alone to more objects than before it ran, which
    /// the other nodes, without the index, couldn't follow. DDL that changes the index alone stays on this node.
    /// </summary>
    /// <param name="before">The ties before the DDL ran.</param>
    internal static void CheckTies(Dictionary<uint, (string Name, bool Partition, bool Identity, bool Clustered, long Dependencies)>? before)
    {
        if (before is null || before.Count == 0)
        {
            return;
        }

        NativeMethods.CommandCounterIncrement();
        foreach ((uint relid, (string Name, bool Partition, bool Identity, bool Clustered, long Dependencies) after) in ReadTies())
        {
            if (before.TryGetValue(relid, out (string Name, bool Partition, bool Identity, bool Clustered, long Dependencies) tie) &&
                ((after.Partition && !tie.Partition) || (after.Identity && !tie.Identity) || (after.Clustered && !tie.Clustered) ||
                    after.Dependencies > tie.Dependencies))
            {
                throw new PgException("55000",
                    $"cannot make objects on every node use index \"{tie.Name}\", which is on this node alone, while worf.enable_ddl_replication is on",
                    hint: "A concurrent build or reindex that failed on this node left the index. Drop it, and build it again so that every node has it.");
            }
        }
    }

    private static Dictionary<uint, (string Name, bool Partition, bool Identity, bool Clustered, long Dependencies)> ReadTies()
    {
        Dictionary<uint, (string Name, bool Partition, bool Identity, bool Clustered, long Dependencies)> ties = [];
        SpiResult result = Elevated.Run(() => Worf.Read("""
            SELECT l.indexrelid, c.relname::pg_catalog.text, c.relispartition, i.indisreplident, i.indisclustered,
                (SELECT pg_catalog.count(*) FROM pg_catalog.pg_depend d
                    WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = l.indexrelid
                        AND (d.refclassid, d.refobjid) <> ('pg_catalog.pg_class'::pg_catalog.regclass, i.indrelid)) +
                (SELECT pg_catalog.count(*) FROM pg_catalog.pg_depend d
                    WHERE d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjid = l.indexrelid)
            FROM worf.local_index l
                JOIN pg_catalog.pg_class c ON c.oid = l.indexrelid
                JOIN pg_catalog.pg_index i ON i.indexrelid = l.indexrelid
            """, 0));
        foreach (SpiRow row in result)
        {
            ties[row.Get<uint>(0)] = (row.Get<string>(1), row.Get<bool>(2), row.Get<bool>(3), row.Get<bool>(4), row.Get<long>(5));
        }

        return ties;
    }

    /// <summary>
    /// Moves the indexes on this node alone out of the way of DDL from another node, which, without them, may create
    /// objects under their names, or choose names for its own that would otherwise differ from the other nodes'.
    /// </summary>
    /// <returns>The indexes moved, by their schemas, names and names meanwhile, for <see cref="MoveBack"/>.</returns>
    internal static List<(uint Namespace, string Name, string Aside)> MoveAside()
    {
        List<(uint Namespace, string Name, string Aside)> moved = [];
        if (Worf.CatalogOid("local_index") == 0)
        {
            return moved;
        }

        SpiResult result = Elevated.Run(() => Worf.Read("SELECT indexrelid FROM worf.local_index", 0));
        foreach (SpiRow row in result)
        {
            uint relid = row.Get<uint>(0);
            sbyte* name = NativeMethods.get_rel_name(relid);
            if (name is null)
            {
                continue;
            }

            uint nspoid = NativeMethods.get_rel_namespace(relid);
            sbyte* aside = NativeMethods.ChooseRelationName(name, null, Worf.PStrDup("worf_aside"), nspoid, false);
            moved.Add((nspoid, Worf.FromServer(name), Worf.FromServer(aside)));
            NativeMethods.RenameRelationInternal(relid, aside, true, true);
            NativeMethods.CommandCounterIncrement();
        }

        return moved;
    }

    /// <summary>
    /// Gives the indexes on this node alone that DDL from another node ran without their names back, or where the DDL
    /// took one, a new name, with a warning.
    /// </summary>
    /// <param name="moved">The indexes moved.</param>
    /// <param name="origin">The node the DDL came from.</param>
    internal static void MoveBack(List<(uint Namespace, string Name, string Aside)> moved, string origin)
    {
        foreach ((uint nspoid, string name, string aside) in moved)
        {
            uint relid = NativeMethods.get_relname_relid(Worf.PStrDup(aside), nspoid);
            if (relid == 0)
            {
                continue;
            }

            sbyte* target = Worf.PStrDup(name);
            if (NativeMethods.get_relname_relid(target, nspoid) != 0)
            {
                target = NativeMethods.ChooseRelationName(target, null, Worf.PStrDup("local"), nspoid, false);
                PgLog.Warning($"renamed index \"{name}\", which is on this node alone, to \"{Worf.FromServer(target)}\", as DDL from node {origin} took its name");
            }

            NativeMethods.RenameRelationInternal(relid, target, true, true);
            NativeMethods.CommandCounterIncrement();
        }
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
            s_namedBefore.Clear();
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
            uint replaced = s_namedBefore.TryGetValue((NativeMethods.IndexGetRelation(relid, false), Worf.FromServer(current)), out uint before) &&
                before != relid ? before : 0;
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

    private static void RegisterCallback()
    {
        if (!s_callbackRegistered)
        {
            NativeMethods.RegisterXactCallback(TransactionCallback, null);
            s_callbackRegistered = true;
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
