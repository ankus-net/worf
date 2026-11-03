using Ankus.Postgres;
using System.Text;

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
/// its name, and they're kept out of the way of other DDL, which neither chooses names by them nor copies, attaches or
/// otherwise uses them, so that it does on every node what it does on the others.
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
    // again for a column's new type, with the indexes themselves.
    private static readonly List<(uint Table, string Name, uint Index)> s_rebuilding = [];

    // The indexes that the transaction built again in place of local indexes, by those they replace.
    private static readonly Dictionary<uint, uint> s_rebuiltAs = [];

    // The local indexes that a concurrent drop drops, whose rows go with them in its last transaction, as its first
    // transaction can't write.
    private static readonly List<uint> s_droppingConcurrently = [];

    // Whether the indexes on this node alone are out of a statement's way, and those moved aside and hidden from it,
    // and whether those it builds again are hidden too.
    private static bool s_isolating;
    private static bool s_hidingRebuilt;
    private static List<(uint Index, string Name)> s_moved = [];
    private static List<(uint Index, uint Table, bool Unlive, uint Filenode)> s_hidden = [];

    // The tables whose indexes on this node alone a statement hides as not live, which nothing may write to meanwhile.
    private static readonly HashSet<uint> s_unliveTables = [];

    // The names of the indexes on this node alone moved out of the way of DDL, by their schemas and the names they have
    // meanwhile, and their schemas.
    private static readonly Dictionary<(uint Schema, string Name), string> s_asideNames = [];
    private static readonly Dictionary<uint, uint> s_movedSchemas = [];

    // RVR_MISSING_OK from catalog/namespace.h, and CREATE_TABLE_LIKE_INDEXES from nodes/parsenodes.h.
    private const uint RangeVarMissingOk = 1;
    private const uint CreateTableLikeIndexes = 1 << 6;

    // SECURITY_LOCAL_USERID_CHANGE from miscadmin.h.
    private const int SecurityLocalUseridChange = 0x0001;

    [PgNativeCallback(nameof(TransactionEnding))]
    private static partial XactCallback TransactionCallback { get; }

    /// <summary>
    /// Gets or sets whether DDL that every node runs as it is, as apply runs another node's or replicate_ddl_command runs
    /// here, runs now, which keeps the indexes on this node alone out of its sight.
    /// </summary>
    internal static bool Applying { get; set; }

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

        if (s_isolating && s_hidingRebuilt && s_rebuilding.Count > 0)
        {
            HideRebuilt(relid);
        }
    }

    /// <summary>
    /// Hides an index that ALTER TABLE builds again in place of a local index it dropped, as not valid, as soon as it's
    /// created, so that no foreign key that the statement adds, or builds again for a column's new type, uses it:
    /// PostgreSQL builds indexes again before it adds constraints. The relation is told from its relcache entry, as its
    /// catalog rows can't be read yet, and once it's one, they're made visible a step before <c>index_create</c> makes
    /// them visible itself, so that they can be changed.
    /// </summary>
    private static void HideRebuilt(uint relid)
    {
        RelationData* rel = NativeMethods.RelationIdGetRelation(relid);
        if (rel is null)
        {
            return;
        }

        bool index = rel->rd_rel->relkind == (sbyte)'i';
        string name = Worf.FromServer((sbyte*)&rel->rd_rel->relname);
        uint nspoid = rel->rd_rel->relnamespace;
        NativeMethods.RelationClose(rel);
        if (!index || !s_rebuilding.Exists(dropped => dropped.Name == name && NativeMethods.get_rel_namespace(dropped.Table) == nspoid))
        {
            return;
        }

        NativeMethods.CommandCounterIncrement();
        WorfRows result = Elevated.Run(() => Worf.Query(
            "UPDATE pg_catalog.pg_index SET indisvalid = false WHERE indexrelid = $1 AND indisvalid RETURNING indrelid", SpiParameter.Create(relid)));
        foreach (WorfRow row in result)
        {
            s_hidden.Add((relid, row.Get<uint>(0), false, 0));
            NativeMethods.CacheInvalidateRelcacheByRelid(row.Get<uint>(0));
        }
    }

    /// <summary>
    /// Forgets a local index as it's dropped, from the object access hook, whatever its schema, and notes one that
    /// PostgreSQL drops to build again, which it does under the same name in the same statement. A concurrent drop
    /// forgets it in its last transaction, as its first can't write.
    /// </summary>
    /// <param name="relid">The relation.</param>
    /// <param name="internalDrop">Whether PostgreSQL drops it on its own, rather than as asked.</param>
    /// <param name="concurrent">Whether it's dropped concurrently.</param>
    internal static void NoteDropped(uint relid, bool internalDrop, bool concurrent)
    {
        if (NativeMethods.get_rel_relkind(relid) != (sbyte)'i' || !Contains(relid))
        {
            return;
        }

        // The index built again is tracked as the transaction commits, if not at the transaction's next statement, so
        // that other sessions find it on this node alone too.
        RegisterCallback();
        if (internalDrop)
        {
            s_rebuilding.Add((NativeMethods.IndexGetRelation(relid, false), Worf.FromServer(NativeMethods.get_rel_name(relid)), relid));
        }

        if (concurrent)
        {
            s_droppingConcurrently.Add(relid);
            return;
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
        List<(uint Table, string Name, uint Index)> rebuilding = [.. s_rebuilding];
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
            uint table = NativeMethods.IndexGetRelation(relid, false);
            int rebuilt = rebuilding.FindIndex(dropped => dropped.Table == table && dropped.Name == name);
            if (s_tracking)
            {
                s_built.Add((relid, name));
                local.Add(relid);
            }
            else if (rebuilt >= 0)
            {
                s_rebuiltAs[rebuilding[rebuilt].Index] = relid;
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
        WorfRows result = Worf.Read("""
            SELECT i.indrelid, i.indexrelid, c.relname::pg_catalog.text FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
            WHERE i.indrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[])
            """, 0, SpiParameter.Create(tables.Select(table => (long)table).ToArray()));
        foreach (WorfRow row in result)
        {
            s_namedBefore[(row.Get<uint>(0), row.Get<string>(2))] = row.Get<uint>(1);
        }
    }

    /// <summary>
    /// Reads how the indexes on this node alone are tied to other objects, before DDL that replicates and may tie one
    /// to them: as a partition of another index, its table's replica identity or clustering index, or by what depends
    /// on it, or it on another index or a constraint, as a constraint that it becomes the index of, or a foreign key
    /// that references its table by it. Objects are told by their descriptions, which an index that the DDL builds
    /// again keeps.
    /// </summary>
    /// <param name="parsetree">The DDL.</param>
    /// <returns>The ties, for <see cref="CheckTies"/>, or null when the DDL can't tie any.</returns>
    internal static Dictionary<uint, (string Name, string?[] Ties)>? Ties(Node* parsetree)
        => (parsetree->type is NodeTag.T_AlterTableStmt or NodeTag.T_CreateStmt ||
                (parsetree->type == NodeTag.T_IndexStmt && !((IndexStmt*)parsetree)->concurrent)) && Worf.CatalogOid("local_index") != 0
            ? ReadTies()
            : null;

    /// <summary>
    /// Fails DDL that replicates and has tied an index on this node alone, or the index it built again in its place,
    /// to objects it wasn't tied to before it ran, which the other nodes, without the index, couldn't follow. DDL that
    /// changes the index alone stays on this node.
    /// </summary>
    /// <param name="before">The ties before the DDL ran.</param>
    internal static void CheckTies(Dictionary<uint, (string Name, string?[] Ties)>? before)
    {
        if (before is null || before.Count == 0)
        {
            return;
        }

        NativeMethods.CommandCounterIncrement();
        Flush();
        Dictionary<uint, uint> replaced = s_rebuiltAs.ToDictionary(rebuilt => rebuilt.Value, rebuilt => rebuilt.Key);
        foreach ((uint relid, (string Name, string?[] Ties) after) in ReadTies())
        {
            uint was = relid;
            while (!before.ContainsKey(was) && replaced.TryGetValue(was, out uint old))
            {
                was = old;
            }

            if (before.TryGetValue(was, out (string Name, string?[] Ties) tied) && after.Ties.Except(tied.Ties).Any())
            {
                throw new PgException("55000",
                    $"cannot make objects on every node use index \"{tied.Name}\", which is on this node alone, while worf.enable_ddl_replication is on",
                    hint: "A concurrent build or reindex that failed on this node left the index. Drop it, and build it again so that every node has it.");
            }
        }
    }

    private static Dictionary<uint, (string Name, string?[] Ties)> ReadTies()
    {
        Dictionary<uint, (string Name, string?[] Ties)> ties = [];
        WorfRows result = Elevated.Run(() => Worf.Read("""
            SELECT l.indexrelid, c.relname::pg_catalog.text, ARRAY(
                SELECT 'partition' WHERE c.relispartition
                UNION ALL SELECT 'replica identity' WHERE i.indisreplident
                UNION ALL SELECT 'clustering' WHERE i.indisclustered
                UNION ALL SELECT 'on ' || pg_catalog.pg_describe_object(d.refclassid, d.refobjid, d.refobjsubid) FROM pg_catalog.pg_depend d
                    WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = l.indexrelid
                        AND d.refclassid IN ('pg_catalog.pg_class'::pg_catalog.regclass, 'pg_catalog.pg_constraint'::pg_catalog.regclass)
                        AND (d.refclassid, d.refobjid) <> ('pg_catalog.pg_class'::pg_catalog.regclass, i.indrelid)
                UNION ALL SELECT 'of ' || pg_catalog.pg_describe_object(d.classid, d.objid, d.objsubid) FROM pg_catalog.pg_depend d
                    WHERE d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjid = l.indexrelid)
            FROM worf.local_index l
                JOIN pg_catalog.pg_class c ON c.oid = l.indexrelid
                JOIN pg_catalog.pg_index i ON i.indexrelid = l.indexrelid
            """, 0));
        foreach (WorfRow row in result)
        {
            ties[row.Get<uint>(0)] = (row.Get<string>(1), row.Get<string?[]>(2));
        }

        return ties;
    }

    /// <summary>
    /// Moves the indexes on this node alone out of the way of DDL from another node, or that replicate_ddl_command runs,
    /// which, without them, may create objects under their names, or choose names for its own that would otherwise
    /// differ from the other nodes'. Only those whose names the DDL could take are moved, as each is locked until the
    /// transaction ends, which holds up other DDL on it, and those that its statements will hide are locked with them,
    /// in one order, so that DDL here and DDL from another node don't deadlock on them.
    /// </summary>
    /// <param name="command">The DDL.</param>
    /// <param name="role">The role that runs it, as which its names are found.</param>
    /// <returns>The indexes moved, with their names, for <see cref="MoveBack"/>.</returns>
    internal static List<(uint Index, string Name)> MoveAside(string command, string role)
    {
        if (Worf.CatalogOid("local_index") == 0)
        {
            return [];
        }

        // The statement's names are found as the role that ran it finds them, as its own search path's "$user" does,
        // as that user rather than with the role set, which a security-definer function can't do. A statement that may
        // set something, as the search path, changes where the ones after it find names, so theirs are looked for in
        // every schema.
        List<(uint Schema, string Name, bool Prefix)>? names = [];
        List<uint> hiding = [];
        uint userId;
        int secContext;
        NativeMethods.GetUserIdAndSecContext(&userId, &secContext);
        NativeMethods.SetUserIdAndSecContext(NativeMethods.get_role_oid(Worf.PStrDup(role), false), secContext | SecurityLocalUseridChange);
        try
        {
            List* statements = NativeMethods.raw_parser(Worf.PStrDup(command), RawParseMode.RAW_PARSE_DEFAULT);
            bool pathKnown = true;
            for (int i = 0; statements is not null && i < statements->length; i++)
            {
                Node* statement = ((RawStmt*)statements->elements[i].ptr_value)->stmt;
                if (names is not null)
                {
                    List<(uint Schema, string Name, bool Prefix)>? taken = NamesTaken(statement, pathKnown);
                    names = taken is null ? null : [.. names, .. taken];
                }

                UsedTables(statement, hiding, hiding);
                if (pathKnown)
                {
                    LockTables(statement);
                }

                pathKnown = pathKnown && !MaySet(statement);
            }
        }
        finally
        {
            NativeMethods.SetUserIdAndSecContext(userId, secContext);
        }

        return MoveAside(names, hiding);
    }

    /// <summary>
    /// Takes the locks a statement takes on the tables it alters, indexes, renames or copies the definition of, as it
    /// would, before the locks on the indexes on this node alone that it moves aside or hides, so that it waits for a
    /// transaction that holds one of them, as on its own, rather than hold the indexes meanwhile, which that transaction
    /// may go on to need.
    /// </summary>
    private static void LockTables(Node* statement)
    {
        switch (statement->type)
        {
            case NodeTag.T_AlterTableStmt:
                AlterTableStmt* alter = (AlterTableStmt*)statement;
                _ = NativeMethods.RangeVarGetRelidExtended(alter->relation, NativeMethods.AlterTableGetLockLevel(alter->cmds), RangeVarMissingOk, default, null);
                break;

            case NodeTag.T_IndexStmt:
                IndexStmt* index = (IndexStmt*)statement;
                _ = NativeMethods.RangeVarGetRelidExtended(index->relation,
                    (int)(index->concurrent ? PgLockMode.ShareUpdateExclusive : PgLockMode.Share), RangeVarMissingOk, default, null);
                break;

            case NodeTag.T_RenameStmt when ((RenameStmt*)statement)->relation is not null:
                _ = NativeMethods.RangeVarGetRelidExtended(((RenameStmt*)statement)->relation, (int)PgLockMode.AccessExclusive, RangeVarMissingOk, default, null);
                break;

            case NodeTag.T_CreateStmt:
                List* elements = ((CreateStmt*)statement)->tableElts;
                for (int i = 0; elements is not null && i < elements->length; i++)
                {
                    Node* element = (Node*)elements->elements[i].ptr_value;
                    if (element->type == NodeTag.T_TableLikeClause)
                    {
                        _ = NativeMethods.RangeVarGetRelidExtended(((TableLikeClause*)element)->relation, (int)PgLockMode.AccessShare,
                            RangeVarMissingOk, default, null);
                    }
                }

                break;

            default:
                break;
        }
    }

    /// <summary>
    /// Names an index on this node alone that a failure of DDL from another node names, as moved out of its way, by its
    /// own name, so that the failure reads as it would without the move.
    /// </summary>
    /// <param name="error">The failure.</param>
    /// <param name="moved">The indexes moved, with their own names.</param>
    /// <returns>The failure, with the indexes' own names.</returns>
    internal static PgException WithOwnNames(PgException error, List<(uint Index, string Name)> moved)
    {
        if (error.ConstraintName is not string aside || moved.Count == 0)
        {
            return error;
        }

        // The names the indexes were moved to, rather than their catalog rows, which a failed transaction can't read.
        foreach ((uint relid, string name) in moved)
        {
            uint nspoid = s_movedSchemas.TryGetValue(relid, out uint schema) ? schema : 0;
            if (s_asideNames.TryGetValue((nspoid, aside), out string? own) && own == name)
            {
                return new PgException(error.SqlState, error.Message.Replace($"\"{aside}\"", $"\"{name}\"", StringComparison.Ordinal),
                    detail: error.Detail, hint: error.Hint);
            }
        }

        return error;
    }

    /// <summary>
    /// Checks whether DDL from another node writes rows to existing tables, or may, whose unique indexes on this node
    /// alone then find duplicates in them, rather than in an index built again.
    /// </summary>
    /// <param name="command">The DDL.</param>
    /// <returns>Whether it may.</returns>
    internal static bool WritesRows(string command)
    {
        List* statements = NativeMethods.raw_parser(Worf.PStrDup(command), RawParseMode.RAW_PARSE_DEFAULT);
        for (int i = 0; statements is not null && i < statements->length; i++)
        {
            Node* statement = ((RawStmt*)statements->elements[i].ptr_value)->stmt;
            if (MaySet(statement) && statement->type is not (NodeTag.T_VariableSetStmt or NodeTag.T_CreateTableAsStmt) &&
                !(statement->type == NodeTag.T_SelectStmt && Into((SelectStmt*)statement) is not null))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Checks whether a statement may set something, as the search path, through a function it runs or itself.
    /// </summary>
    private static bool MaySet(Node* statement) => statement->type is NodeTag.T_VariableSetStmt or NodeTag.T_SelectStmt or
        NodeTag.T_InsertStmt or NodeTag.T_UpdateStmt or NodeTag.T_DeleteStmt or NodeTag.T_MergeStmt or NodeTag.T_ExplainStmt or
        NodeTag.T_CreateTableAsStmt or NodeTag.T_CopyStmt or NodeTag.T_DoStmt or NodeTag.T_CallStmt or NodeTag.T_ExecuteStmt;

    /// <summary>
    /// Moves the indexes on this node alone whose names a statement could take out of its way, after it locks them and
    /// those of the tables whose indexes it will hide, in the order of their OIDs.
    /// </summary>
    /// <param name="names">The names, or null for every name.</param>
    /// <param name="hiding">The tables whose indexes on this node alone the statement will hide.</param>
    /// <returns>The indexes moved, with their names.</returns>
    private static List<(uint Index, string Name)> MoveAside(List<(uint Schema, string Name, bool Prefix)>? names, List<uint> hiding)
    {
        List<(uint Index, string Name)> moved = [];
        if (names is { Count: 0 } && hiding.Count == 0)
        {
            return moved;
        }

        WorfRows result = Elevated.Run(() => Worf.Read("""
            SELECT l.indexrelid, i.indrelid FROM worf.local_index l JOIN pg_catalog.pg_index i ON i.indexrelid = l.indexrelid
            ORDER BY 1
            """, 0));
        List<uint> moving = [];
        foreach ((uint relid, uint table) in result.Select(row => (row.Get<uint>(0), row.Get<uint>(1))))
        {
            bool takes = Takes(names, relid);
            if (takes || hiding.Contains(table))
            {
                NativeMethods.LockRelationOid(relid, (int)PgLockMode.ShareUpdateExclusive);
            }

            // Its name is read again once it's locked, as a rename may have committed meanwhile.
            if (takes && Takes(names, relid))
            {
                moving.Add(relid);
            }
        }

        foreach (uint relid in moving)
        {
            sbyte* name = NativeMethods.get_rel_name(relid);
            uint nspoid = NativeMethods.get_rel_namespace(relid);
            if (name is null)
            {
                continue;
            }

            moved.Add((relid, Worf.FromServer(name)));
            sbyte* aside = NativeMethods.ChooseRelationName(name, null, Worf.PStrDup("worf_aside"), nspoid, false);
            s_asideNames[(nspoid, Worf.FromServer(aside))] = Worf.FromServer(name);
            s_movedSchemas[relid] = nspoid;
            NativeMethods.RenameRelationInternal(relid, aside, true, true);
            NativeMethods.CommandCounterIncrement();
        }

        return moved;
    }

    /// <summary>
    /// Checks whether a statement could take an index's name: one it gives, or one PostgreSQL chooses from a table's,
    /// which starts with as much of it as fits beside the rest, at least 20 bytes.
    /// </summary>
    private static bool Takes(List<(uint Schema, string Name, bool Prefix)>? names, uint relid)
    {
        sbyte* current = NativeMethods.get_rel_name(relid);
        if (current is null)
        {
            return false;
        }

        if (names is null)
        {
            return true;
        }

        string name = Worf.FromServer(current);
        uint nspoid = NativeMethods.get_rel_namespace(relid);
        return names.Exists(taken => (taken.Schema == 0 || taken.Schema == nspoid) &&
            (taken.Prefix ? name.StartsWith(ChosenStart(taken.Name), StringComparison.Ordinal) : name == taken.Name));
    }

    /// <summary>
    /// Gives the start of a table's name that the names PostgreSQL chooses from it keep: <c>makeObjectName</c> cuts the
    /// longer of the two names it joins until they fit, so the table's keeps at least half of what the label leaves, and
    /// puts an underscore after it, so a table's name that short is kept whole, with the underscore.
    /// </summary>
    private static string ChosenStart(string table)
    {
        const int Kept = 20;
        int bytes = 0;
        int at = 0;
        while (at < table.Length)
        {
            int length = char.IsSurrogatePair(table, at) ? 2 : 1;
            bytes += Encoding.UTF8.GetByteCount(table.AsSpan(at, length));
            if (bytes > Kept)
            {
                break;
            }

            at += length;
        }

        return at == table.Length ? table + "_" : table[..at];
    }

    /// <summary>
    /// Finds the names a statement may give relations, which an index on this node alone could have: those it gives
    /// relations it creates or renames, or that a table, index or type it moves to another schema has, with the
    /// indexes and sequences that move with it, and the starts of those PostgreSQL chooses from the tables it creates,
    /// alters, indexes or attaches, and from their partitions, for their indexes and sequences. A statement that can
    /// create relations anywhere, as a DO block, a procedure or an extension can, may take any name, and any other, as
    /// a function's, a DROP or a statement that names no relation, none.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <param name="pathKnown">Whether the search path the statement finds names on is the current one.</param>
    /// <returns>The names, in their schemas or, with a zero schema, in any, or null for every name.</returns>
    private static List<(uint Schema, string Name, bool Prefix)>? NamesTaken(Node* parsetree, bool pathKnown)
    {
        List<(uint Schema, string Name, bool Prefix)> names = [];
        switch (parsetree->type)
        {
            // EXPLAIN ANALYZE runs the statement it explains.
            case NodeTag.T_ExplainStmt:
                return NamesTaken(((ExplainStmt*)parsetree)->query, pathKnown);

            // A statement that runs functions, as a DO block, a procedure, an extension's script or a query does, may
            // create relations under any name.
            case NodeTag.T_DoStmt or NodeTag.T_CallStmt or NodeTag.T_CreateExtensionStmt or NodeTag.T_AlterExtensionStmt or
                NodeTag.T_ImportForeignSchemaStmt or NodeTag.T_SelectStmt or NodeTag.T_InsertStmt or NodeTag.T_UpdateStmt or
                NodeTag.T_DeleteStmt or NodeTag.T_MergeStmt or NodeTag.T_CopyStmt or NodeTag.T_ExecuteStmt or NodeTag.T_CreateTableAsStmt:
                return null;

            case NodeTag.T_CreateStmt or NodeTag.T_CreateForeignTableStmt:
                CreateStmt* create = (CreateStmt*)parsetree;
                uint schema = SchemaOf(create->relation, true, pathKnown);
                string table = Worf.FromServer(create->relation->relname);
                names.Add((schema, table, false));
                names.Add((schema, table, true));
                AddConstraintNames(names, schema, create->tableElts);
                break;

            case NodeTag.T_IndexStmt:
                IndexStmt* index = (IndexStmt*)parsetree;
                if (index->idxname is not null)
                {
                    names.Add((SchemaOf(index->relation, false, pathKnown), Worf.FromServer(index->idxname), false));
                }

                AddTreeNames(names, index->relation, pathKnown);
                break;

            case NodeTag.T_AlterTableStmt when ((AlterTableStmt*)parsetree)->objtype is ObjectType.OBJECT_TABLE or ObjectType.OBJECT_FOREIGN_TABLE:
                AlterTableStmt* alter = (AlterTableStmt*)parsetree;
                AddTreeNames(names, alter->relation, pathKnown);
                uint altered = SchemaOf(alter->relation, false, pathKnown);
                List* cmds = alter->cmds;
                for (int i = 0; cmds is not null && i < cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
                    if (cmd->subtype == AlterTableType.AT_AttachPartition && cmd->def is not null)
                    {
                        AddTreeNames(names, ((PartitionCmd*)cmd->def)->name, pathKnown);
                    }
                    else if (cmd->subtype is AlterTableType.AT_AddConstraint or AlterTableType.AT_AddColumn or AlterTableType.AT_AddIdentity &&
                        cmd->def is not null)
                    {
                        AddConstraintNames(names, altered, cmd->def);
                    }
                }

                break;

            case NodeTag.T_CreateSeqStmt:
                names.Add((SchemaOf(((CreateSeqStmt*)parsetree)->sequence, true, pathKnown), Worf.FromServer(((CreateSeqStmt*)parsetree)->sequence->relname), false));
                break;

            case NodeTag.T_ViewStmt:
                names.Add((SchemaOf(((ViewStmt*)parsetree)->view, true, pathKnown), Worf.FromServer(((ViewStmt*)parsetree)->view->relname), false));
                break;

            case NodeTag.T_CompositeTypeStmt:
                names.Add((SchemaOf(((CompositeTypeStmt*)parsetree)->typevar, true, pathKnown), Worf.FromServer(((CompositeTypeStmt*)parsetree)->typevar->relname), false));
                break;

            case NodeTag.T_RenameStmt:
                return RenamedNames((RenameStmt*)parsetree, pathKnown);

            case NodeTag.T_AlterObjectSchemaStmt:
                return MovedNames((AlterObjectSchemaStmt*)parsetree, pathKnown);

            default:
                break;
        }

        return names;
    }

    /// <summary>
    /// Finds the name a rename gives a relation, a composite type's relation or the index of a constraint.
    /// </summary>
    private static List<(uint Schema, string Name, bool Prefix)>? RenamedNames(RenameStmt* rename, bool pathKnown)
    {
        string newname = rename->newname is null ? "" : Worf.FromServer(rename->newname);
        switch (rename->renameType)
        {
            case ObjectType.OBJECT_TABLE or ObjectType.OBJECT_INDEX or ObjectType.OBJECT_SEQUENCE or ObjectType.OBJECT_VIEW or
                ObjectType.OBJECT_MATVIEW or ObjectType.OBJECT_FOREIGN_TABLE or ObjectType.OBJECT_TABCONSTRAINT:
                return [(SchemaOf(rename->relation, false, pathKnown), newname, false)];

            case ObjectType.OBJECT_TYPE:
                uint typid = NativeMethods.LookupTypeNameOid(null, NativeMethods.makeTypeNameFromNameList((List*)rename->@object), true);
                uint typrelid = typid == 0 ? 0 : NativeMethods.get_typ_typrelid(typid);
                return typid == 0 ? null : typrelid == 0 ? [] : [(pathKnown ? NativeMethods.get_rel_namespace(typrelid) : 0, newname, false)];

            default:
                return [];
        }
    }

    /// <summary>
    /// Finds the names that a relation, a composite type's relation or an extension's relations moved to another
    /// schema take there, with those of the indexes and sequences that move with them.
    /// </summary>
    private static List<(uint Schema, string Name, bool Prefix)>? MovedNames(AlterObjectSchemaStmt* move, bool pathKnown)
    {
        uint target = NativeMethods.get_namespace_oid(move->newschema, true);
        uint relid;
        switch (move->objectType)
        {
            case ObjectType.OBJECT_TABLE or ObjectType.OBJECT_SEQUENCE or ObjectType.OBJECT_VIEW or ObjectType.OBJECT_MATVIEW or
                ObjectType.OBJECT_FOREIGN_TABLE:
                relid = move->relation is null || !(pathKnown || move->relation->schemaname is not null) ? 0
                    : NativeMethods.RangeVarGetRelidExtended(move->relation, 0, RangeVarMissingOk, default, null);
                break;

            case ObjectType.OBJECT_TYPE:
                uint typid = NativeMethods.LookupTypeNameOid(null, NativeMethods.makeTypeNameFromNameList((List*)move->@object), true);
                relid = typid == 0 ? 0 : NativeMethods.get_typ_typrelid(typid);
                if (typid != 0 && relid == 0)
                {
                    return [];
                }

                break;

            case ObjectType.OBJECT_EXTENSION:
                RelationData* rel = null;
                ObjectAddress extension = NativeMethods.get_object_address(ObjectType.OBJECT_EXTENSION, move->@object, &rel, (int)PgLockMode.AccessShare, true);
                return target == 0 || extension.objectId == 0 ? null : [.. Elevated.Run(() => Worf.Read("""
                    WITH m AS (
                        SELECT c.oid, c.relname FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_class c ON c.oid = d.objid
                        WHERE d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND d.refobjid = $1
                            AND d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.deptype = 'e'
                        UNION
                        SELECT c.oid, c.relname FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_type t ON t.oid = d.objid
                            JOIN pg_catalog.pg_class c ON c.oid = t.typrelid
                        WHERE d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND d.refobjid = $1
                            AND d.classid = 'pg_catalog.pg_type'::pg_catalog.regclass AND d.deptype = 'e'
                    )
                    SELECT relname::pg_catalog.text FROM m
                    UNION
                    SELECT c.relname::pg_catalog.text FROM m JOIN pg_catalog.pg_index i ON i.indrelid = m.oid JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
                    """, 0, SpiParameter.Create(extension.objectId))).Select(row => (target, row.Get<string>(0), false))];

            default:
                return [];
        }

        return target == 0 || relid == 0 ? null : [.. Elevated.Run(() => Worf.Read("""
            SELECT relname::pg_catalog.text FROM pg_catalog.pg_class WHERE oid = $1
            UNION
            SELECT c.relname::pg_catalog.text FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = $1
            UNION
            SELECT c.relname::pg_catalog.text FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_class c ON c.oid = d.objid
            WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
                AND d.refobjid = $1 AND d.deptype IN ('a', 'i') AND c.relkind = 'S'
            """, 0, SpiParameter.Create(relid))).Select(row => (target, row.Get<string>(0), false))];
    }

    /// <summary>
    /// Finds the table that <c>SELECT ... INTO</c> creates, which the first SELECT of a set operation names.
    /// </summary>
    private static IntoClause* Into(SelectStmt* select)
    {
        while (select->op != SetOperation.SETOP_NONE && select->larg is not null)
        {
            select = select->larg;
        }

        return select->intoClause;
    }

    /// <summary>
    /// Adds the names that the unique, primary key and exclusion constraints a table's columns or table elements
    /// define give their indexes.
    /// </summary>
    private static void AddConstraintNames(List<(uint Schema, string Name, bool Prefix)> names, uint schema, List* elements)
    {
        for (int i = 0; elements is not null && i < elements->length; i++)
        {
            AddConstraintNames(names, schema, (Node*)elements->elements[i].ptr_value);
        }
    }

    private static void AddConstraintNames(List<(uint Schema, string Name, bool Prefix)> names, uint schema, Node* element)
    {
        if (element->type == NodeTag.T_ColumnDef)
        {
            AddConstraintNames(names, schema, ((ColumnDef*)element)->constraints);
        }
        else if (element->type == NodeTag.T_Constraint && ((Constraint*)element)->conname is not null &&
            ((Constraint*)element)->contype is ConstrType.CONSTR_PRIMARY or ConstrType.CONSTR_UNIQUE or ConstrType.CONSTR_EXCLUSION)
        {
            names.Add((schema, Worf.FromServer(((Constraint*)element)->conname), false));
        }
        else if (element->type == NodeTag.T_Constraint && ((Constraint*)element)->contype == ConstrType.CONSTR_IDENTITY)
        {
            // An identity column's sequence takes the name its options give, in the schema they give or the table's.
            List* options = ((Constraint*)element)->options;
            for (int i = 0; options is not null && i < options->length; i++)
            {
                DefElem* option = (DefElem*)options->elements[i].ptr_value;
                if (Worf.FromServer(option->defname) == "sequence_name" && option->arg is not null && option->arg->type == NodeTag.T_List)
                {
                    sbyte* nspname;
                    sbyte* objname;
                    NativeMethods.DeconstructQualifiedName((List*)option->arg, &nspname, &objname);
                    names.Add((nspname is null ? schema : NativeMethods.get_namespace_oid(nspname, true), Worf.FromServer(objname), false));
                }
            }
        }
    }

    /// <summary>
    /// Adds the starts of the names PostgreSQL chooses from a table and from its partitions and inheritors, in their
    /// schemas, for the indexes a statement builds on them and the sequences it creates for them.
    /// </summary>
    private static void AddTreeNames(List<(uint Schema, string Name, bool Prefix)> names, RangeVar* relation, bool pathKnown)
    {
        uint relid = relation is null || !(pathKnown || relation->schemaname is not null) ? 0
            : NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        if (relid == 0)
        {
            if (relation is not null)
            {
                names.Add((SchemaOf(relation, false, pathKnown), Worf.FromServer(relation->relname), true));
            }

            return;
        }

        List* tree = NativeMethods.find_all_inheritors(relid, (int)PgLockMode.None, null);
        for (int i = 0; tree is not null && i < tree->length; i++)
        {
            uint member = tree->elements[i].oid_value;
            names.Add((NativeMethods.get_rel_namespace(member), Worf.FromServer(NativeMethods.get_rel_name(member)), true));
        }
    }

    /// <summary>
    /// Finds the schema of a relation a statement names: the one it gives, or for a relation it creates, the one it
    /// creates it in, or that of the relation found.
    /// </summary>
    /// <returns>The schema, or zero when it can't be told.</returns>
    private static uint SchemaOf(RangeVar* relation, bool creating, bool pathKnown)
    {
        if (relation is null)
        {
            return 0;
        }

        if (relation->schemaname is not null)
        {
            return NativeMethods.get_namespace_oid(relation->schemaname, true);
        }

        // A relation created without a schema goes in the first schema of the path, where one can be created, and a
        // statement whose path has none, as replicate_ddl_command's empty one, or whose path a statement before it may
        // have set, can't be told.
        if (!pathKnown)
        {
            return 0;
        }

        if (creating)
        {
            List* path = NativeMethods.fetch_search_path(false);
            return path is null || path->length == 0 ? 0 : path->elements[0].oid_value;
        }

        uint relid = NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        return relid == 0 ? 0 : NativeMethods.get_rel_namespace(relid);
    }

    /// <summary>
    /// Gives the indexes on this node alone that DDL ran without their names back, or the indexes the DDL built again
    /// in their places, in whatever schema the DDL moved them to, or where the DDL took one, a new name, with a
    /// warning.
    /// </summary>
    /// <param name="moved">The indexes moved.</param>
    /// <param name="origin">The node the DDL came from, or null for this node's own DDL.</param>
    internal static void MoveBack(List<(uint Index, string Name)> moved, string? origin)
    {
        Flush();
        foreach ((uint relid, string name) in moved)
        {
            uint current = relid;
            while (NativeMethods.get_rel_relkind(current) != (sbyte)'i' && s_rebuiltAs.TryGetValue(current, out uint rebuilt))
            {
                current = rebuilt;
            }

            if (NativeMethods.get_rel_relkind(current) != (sbyte)'i' || Worf.FromServer(NativeMethods.get_rel_name(current)) == name)
            {
                continue;
            }

            uint nspoid = NativeMethods.get_rel_namespace(current);
            sbyte* target = Worf.PStrDup(name);
            if (NativeMethods.get_relname_relid(target, nspoid) != 0)
            {
                target = NativeMethods.ChooseRelationName(target, null, Worf.PStrDup("local"), nspoid, false);
                WorfLog.Warning($"renamed index \"{name}\", which is on this node alone, to \"{Worf.FromServer(target)}\", as DDL{(origin is null ? "" : $" from node {origin}")} took its name");
            }

            NativeMethods.RenameRelationInternal(current, target, true, true);
            NativeMethods.CommandCounterIncrement();
        }
    }

    /// <summary>
    /// Moves the indexes on this node alone out of a statement's way, unless the statement that runs it already has:
    /// out of the way of the names that PostgreSQL chooses for the objects that DDL that replicates creates, so that
    /// every node chooses the same, as <see cref="MoveAside(string, string)"/> does for apply; and out of sight of DDL that replicates,
    /// or DDL from another node, that would copy them, attach them to a partitioned index, or have a foreign key use
    /// them, none of which the other nodes can. Only the indexes of the tables the statement copies, attaches,
    /// indexes the partitions of or references are hidden: those a foreign key or an attached partition would use are
    /// marked not valid, which they're still kept up to date and built again as, and those that a copy or a
    /// partitioned index would take, which only indexes that aren't live escape, are marked not live, while nothing may
    /// write to or rewrite their tables. ALTER TABLE that names no index hides those it builds again as not valid as it
    /// creates them.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <param name="replicated">Whether it's DDL to replicate.</param>
    /// <returns>Whether any are out of its way, for <see cref="EndIsolation"/> and <see cref="Restore"/>.</returns>
    internal static bool Isolate(Node* parsetree, bool replicated)
    {
        bool rename = replicated && ChoosesNames(parsetree);
        bool hidingRebuilt = parsetree->type == NodeTag.T_AlterTableStmt && (Applying || rename);
        List<uint> unlive = [];
        List<uint> invalid = [];
        if (replicated || Applying)
        {
            UsedTables(parsetree, unlive, invalid);
        }

        if (s_isolating || !(rename || hidingRebuilt || unlive.Count > 0 || invalid.Count > 0) || Worf.CatalogOid("local_index") == 0 ||
            !Elevated.Run(() => Worf.ReadScalar<bool>("SELECT EXISTS (SELECT FROM worf.local_index)")))
        {
            return false;
        }

        LockTables(parsetree);
        s_moved = MoveAside(rename ? NamesTaken(parsetree, true) : [], [.. unlive, .. invalid]);
        s_hidden = [];
        Hide(unlive, true);
        Hide(invalid, false);
        s_hidingRebuilt = hidingRebuilt;
        s_isolating = true;
        return true;
    }

    /// <summary>
    /// Ends a statement that <see cref="Isolate"/> ran out of the way of the indexes on this node alone, whether it ran
    /// or failed, which rolls their changes back, so that their tables can be written to again.
    /// </summary>
    /// <param name="isolated">Whether they were out of its way.</param>
    internal static void EndIsolation(bool isolated)
    {
        if (isolated)
        {
            s_isolating = false;
            s_hidingRebuilt = false;
            s_unliveTables.Clear();
        }
    }

    /// <summary>
    /// Brings the indexes on this node alone back once a statement that <see cref="Isolate"/> ran out of their way has
    /// run: live and valid again, and under their names, as <see cref="MoveBack"/> gives them. A statement that
    /// rewrote a table whose indexes weren't live, which left them unbuilt, fails.
    /// </summary>
    /// <param name="isolated">Whether they were out of its way.</param>
    internal static void Restore(bool isolated)
    {
        if (!isolated)
        {
            return;
        }

        List<(uint Index, uint Table, bool Unlive, uint Filenode)> hidden = s_hidden;
        List<(uint Index, string Name)> moved = s_moved;
        s_hidden = [];
        s_moved = [];
        foreach ((uint _, uint table, bool _, uint filenode) in hidden.Where(index => index.Unlive))
        {
            if (Elevated.Run(() => Worf.ReadScalar<uint>("SELECT relfilenode FROM pg_catalog.pg_class WHERE oid = $1", SpiParameter.Create(table))) != filenode)
            {
                throw new PgException("55000",
                    $"cannot rewrite table \"{Worf.FromServer(NativeMethods.get_rel_name(table))}\" while DDL runs that its indexes on this node alone are hidden from",
                    hint: "The indexes wouldn't be built again. Rewrite the table before or after the DDL, not from an event trigger or a function that it runs.");
            }
        }

        foreach (bool unlive in new[] { true, false })
        {
            long[] relids = [.. hidden.Where(index => index.Unlive == unlive).Select(index => (long)index.Index)];
            if (relids.Length == 0)
            {
                continue;
            }

            WorfRows result = Elevated.Run(() => Worf.Query(unlive
                ? "UPDATE pg_catalog.pg_index SET indislive = true WHERE indexrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[]) RETURNING indrelid"
                : "UPDATE pg_catalog.pg_index SET indisvalid = true WHERE indexrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[]) RETURNING indrelid",
                SpiParameter.Create(relids)));
            foreach (WorfRow row in result)
            {
                NativeMethods.CacheInvalidateRelcacheByRelid(row.Get<uint>(0));
            }
        }

        NativeMethods.CommandCounterIncrement();
        MoveBack(moved, null);
    }

    /// <summary>
    /// Fails a write to a table whose indexes on this node alone a statement has hidden as not live, as from an event
    /// trigger or a function that the statement runs, since the indexes wouldn't get its rows. A write to a
    /// partitioned table checks the partitions it can reach.
    /// </summary>
    /// <param name="relids">The tables written to.</param>
    internal static void CheckWrites(List<uint> relids)
    {
        if (s_unliveTables.Count == 0)
        {
            return;
        }

        foreach (uint relid in relids)
        {
            List* tables = NativeMethods.find_all_inheritors(relid, (int)PgLockMode.None, null);
            for (int i = 0; tables is not null && i < tables->length; i++)
            {
                uint table = tables->elements[i].oid_value;
                if (s_unliveTables.Contains(table))
                {
                    throw new PgException("55000",
                        $"cannot write to table \"{Worf.FromServer(NativeMethods.get_rel_name(table))}\" while DDL runs that its indexes on this node alone are hidden from",
                        hint: "The indexes wouldn't get the rows. Write to the table before or after the DDL, not from an event trigger or a function that it runs.");
                }
            }
        }
    }

    /// <summary>
    /// Fails a <c>COPY FROM</c> or <c>TRUNCATE</c> of a table whose indexes on this node alone a statement has hidden as
    /// not live, as <see cref="CheckWrites"/> does.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    internal static void CheckWrite(Node* parsetree)
    {
        if (s_unliveTables.Count == 0)
        {
            return;
        }

        List<uint> relids = [];
        if (parsetree->type == NodeTag.T_CopyStmt && ((CopyStmt*)parsetree)->is_from)
        {
            AddTable(relids, ((CopyStmt*)parsetree)->relation);
        }
        else if (parsetree->type == NodeTag.T_TruncateStmt)
        {
            List* relations = ((TruncateStmt*)parsetree)->relations;
            for (int i = 0; relations is not null && i < relations->length; i++)
            {
                AddTable(relids, (RangeVar*)relations->elements[i].ptr_value);
            }
        }

        CheckWrites(relids);
    }

    /// <summary>
    /// Drops an index on this node alone that DDL from another node couldn't build again, as a unique one that finds
    /// duplicate keys, with a warning that gives its definition, so that the DDL can apply. Such an index can't be
    /// valid on this node, nor exist on the others.
    /// </summary>
    /// <param name="error">The DDL's error.</param>
    /// <param name="moved">The indexes moved out of the DDL's way.</param>
    /// <param name="origin">The node the DDL came from.</param>
    /// <returns>Whether the index was dropped, so that the DDL can run again.</returns>
    internal static bool DropUnbuilt(PgException error, List<(uint Index, string Name)> moved, string origin)
    {
        // The statement's subtransaction rolled back what it created and dropped.
        s_created.Clear();
        s_rebuilding.Clear();
        uint nspoid = error.SchemaName is null ? 0 : NativeMethods.get_namespace_oid(Worf.PStrDup(error.SchemaName), true);
        uint relid = error.SqlState != "23505" || nspoid == 0 || error.ConstraintName is null ? 0
            : NativeMethods.get_relname_relid(Worf.PStrDup(error.ConstraintName), nspoid);
        if (relid == 0 || NativeMethods.get_rel_relkind(relid) != (sbyte)'i' || !Contains(relid))
        {
            return false;
        }

        // The index's definition under the name it has outside apply, which an index built again in its place, under its
        // name meanwhile, has too.
        int found = moved.FindIndex(index => index.Index == relid);
        string name = found >= 0 ? moved[found].Name
            : s_asideNames.TryGetValue((nspoid, error.ConstraintName!), out string? before) ? before
            : error.ConstraintName!;
        string definition = Worf.FromServer(NativeMethods.pg_get_indexdef_string(relid));
        string named = $"INDEX {Worf.QuoteIdentifier(error.ConstraintName!)} ON ";
        int at = definition.IndexOf(named, StringComparison.Ordinal);
        definition = at < 0 ? definition : $"{definition[..at]}INDEX {Worf.QuoteIdentifier(name)} ON {definition[(at + named.Length)..]}";

        ObjectAddress index = Dependency.Address(Dependency.RelationRelationId, relid, 0);
        NativeMethods.performDeletion(&index, DropBehavior.DROP_RESTRICT, 0);
        NativeMethods.CommandCounterIncrement();
        WorfLog.Warning(new PgDiagnostic(
            $"dropped index \"{name}\", which is on this node alone, as DDL from node {origin} couldn't build it again: " +
            error.Message.Replace($"\"{error.ConstraintName}\"", $"\"{name}\"", StringComparison.Ordinal))
        {
            Detail = error.Detail,
            Hint = $"Its definition was: {definition}",
        });
        return true;
    }

    /// <summary>
    /// Names the index that a concurrent build whose DDL replicates built without a name in the DDL to queue, as the
    /// name it chose here, beside the indexes on this node alone, may differ from the one the other nodes would.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <param name="command">The DDL to queue.</param>
    /// <returns>The DDL to queue, naming the index.</returns>
    internal static string NameBuilt(Node* parsetree, string command)
    {
        sbyte* name = parsetree->type != NodeTag.T_IndexStmt || ((IndexStmt*)parsetree)->idxname is not null || s_built.Count == 0
            ? null
            : NativeMethods.get_rel_name(s_built[0].Oid);
        if (name is null)
        {
            return command;
        }

        // CREATE [UNIQUE] INDEX CONCURRENTLY, and ON, which the name goes before.
        int at = 0;
        string[] keywords = ((IndexStmt*)parsetree)->unique ? ["create", "unique", "index", "concurrently"] : ["create", "index", "concurrently"];
        foreach (string keyword in keywords)
        {
            at = SkipSpace(command, at);
            if (at + keyword.Length > command.Length || !command.AsSpan(at, keyword.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase))
            {
                return command;
            }

            at += keyword.Length;
        }

        at = SkipSpace(command, at);
        return command.AsSpan(at).StartsWith("on", StringComparison.OrdinalIgnoreCase)
            ? $"{command[..at]}{Worf.QuoteIdentifier(Worf.FromServer(name))} {command[at..]}"
            : command;
    }

    /// <summary>
    /// Skips the white space and comments in SQL from a position.
    /// </summary>
    private static int SkipSpace(string sql, int at)
    {
        while (at < sql.Length)
        {
            if (char.IsWhiteSpace(sql[at]))
            {
                at++;
            }
            else if (sql.AsSpan(at).StartsWith("--", StringComparison.Ordinal))
            {
                int end = sql.IndexOf('\n', at);
                at = end < 0 ? sql.Length : end + 1;
            }
            else if (sql.AsSpan(at).StartsWith("/*", StringComparison.Ordinal))
            {
                // Block comments nest.
                int depth = 0;
                do
                {
                    if (sql.AsSpan(at).StartsWith("/*", StringComparison.Ordinal))
                    {
                        depth++;
                        at += 2;
                    }
                    else if (sql.AsSpan(at).StartsWith("*/", StringComparison.Ordinal))
                    {
                        depth--;
                        at += 2;
                    }
                    else
                    {
                        at++;
                    }
                }
                while (depth > 0 && at < sql.Length);
            }
            else
            {
                break;
            }
        }

        return at;
    }

    /// <summary>
    /// Checks whether PostgreSQL may choose the names of objects a statement creates, which doesn't commit on its way
    /// nor name an index that it would then find under another name.
    /// </summary>
    private static bool ChoosesNames(Node* parsetree)
    {
        switch (parsetree->type)
        {
            case NodeTag.T_IndexStmt:
                return !((IndexStmt*)parsetree)->concurrent;
            case NodeTag.T_CreateStmt:
                return true;
            case NodeTag.T_AlterTableStmt when ((AlterTableStmt*)parsetree)->objtype is ObjectType.OBJECT_TABLE or ObjectType.OBJECT_FOREIGN_TABLE:
                // An index on this node alone that the statement names has to keep its name for the statement to find it.
                AlterTableStmt* alter = (AlterTableStmt*)parsetree;
                List* cmds = alter->cmds;
                uint relid = NativeMethods.RangeVarGetRelidExtended(alter->relation, 0, RangeVarMissingOk, default, null);
                for (int i = 0; cmds is not null && i < cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
                    sbyte* named = cmd->subtype switch
                    {
                        AlterTableType.AT_ClusterOn => cmd->name,
                        AlterTableType.AT_ReplicaIdentity when cmd->def is not null => ((ReplicaIdentityStmt*)cmd->def)->name,
                        AlterTableType.AT_AddIndexConstraint when cmd->def is not null => ((IndexStmt*)cmd->def)->idxname,
                        AlterTableType.AT_AddConstraint when cmd->def is not null && cmd->def->type == NodeTag.T_Constraint =>
                            ((Constraint*)cmd->def)->indexname,
                        _ => null,
                    };
                    if (named is not null && (relid == 0 || Contains(NativeMethods.get_relname_relid(named, NativeMethods.get_rel_namespace(relid)))))
                    {
                        return false;
                    }
                }

                return true;
            default:
                return false;
        }
    }

    /// <summary>
    /// Finds the tables whose existing indexes a statement may copy, attach to a partitioned index, or have a foreign
    /// key use: the tables that <c>CREATE TABLE ... LIKE</c> copies the indexes of, the partitions of a partitioned
    /// table indexed, which the index may take its partitions from, and those of a partition attached, and the tables
    /// that a partition attached is, or that foreign keys reference, which take only valid indexes.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <param name="unlive">The tables whose indexes are to be hidden as not live.</param>
    /// <param name="invalid">The tables whose indexes are to be hidden as not valid.</param>
    private static void UsedTables(Node* parsetree, List<uint> unlive, List<uint> invalid)
    {
        switch (parsetree->type)
        {
            case NodeTag.T_CreateStmt:
                List* elements = ((CreateStmt*)parsetree)->tableElts;
                for (int i = 0; elements is not null && i < elements->length; i++)
                {
                    Node* element = (Node*)elements->elements[i].ptr_value;
                    if (element->type == NodeTag.T_TableLikeClause && (((TableLikeClause*)element)->options & CreateTableLikeIndexes) != 0)
                    {
                        AddTable(unlive, ((TableLikeClause*)element)->relation);
                    }
                    else
                    {
                        AddReferenced(invalid, element);
                    }
                }

                break;

            case NodeTag.T_IndexStmt when !((IndexStmt*)parsetree)->concurrent && ((IndexStmt*)parsetree)->relation->inh:
                uint indexed = NativeMethods.RangeVarGetRelidExtended(((IndexStmt*)parsetree)->relation, 0, RangeVarMissingOk, default, null);
                if (indexed != 0 && NativeMethods.get_rel_relkind(indexed) == (sbyte)'p')
                {
                    AddPartitions(unlive, indexed);
                }

                break;

            case NodeTag.T_AlterTableStmt when ((AlterTableStmt*)parsetree)->objtype is ObjectType.OBJECT_TABLE or ObjectType.OBJECT_TYPE:
                bool type = ((AlterTableStmt*)parsetree)->objtype == ObjectType.OBJECT_TYPE;
                List* cmds = ((AlterTableStmt*)parsetree)->cmds;
                for (int i = 0; cmds is not null && i < cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
                    if (!type && cmd->subtype == AlterTableType.AT_AttachPartition && cmd->def is not null)
                    {
                        uint attached = AddTable(invalid, ((PartitionCmd*)cmd->def)->name);
                        if (attached != 0)
                        {
                            AddPartitions(unlive, attached);
                        }
                    }
                    else if (!type && cmd->subtype is AlterTableType.AT_AddConstraint or AlterTableType.AT_AddColumn && cmd->def is not null)
                    {
                        AddReferenced(invalid, cmd->def);
                    }
                    else if (Rewrites(cmd->subtype))
                    {
                        // A column's new type or expression builds the table's foreign keys again, and those of its
                        // inheritors and partitions, which may then use any valid unique index on the tables they
                        // reference. An attribute's new type in a composite type does so for every table that uses the
                        // type, which the change cascades to.
                        uint altered = NativeMethods.RangeVarGetRelidExtended(((AlterTableStmt*)parsetree)->relation, 0, RangeVarMissingOk, default, null);
                        if (altered != 0)
                        {
                            AddRebuiltReferences(invalid, type ? TablesUsingType(NativeMethods.get_rel_type_id(altered)) : [altered]);
                        }
                    }
                }

                break;

            default:
                break;
        }
    }

    /// <summary>
    /// Checks whether an ALTER TABLE action rebuilds a table's foreign keys: a column's new type or, from PostgreSQL 17,
    /// generation expression.
    /// </summary>
    private static bool Rewrites(AlterTableType subtype)
#if ANKUS_PG15 || ANKUS_PG16
        => subtype == AlterTableType.AT_AlterColumnType;
#else
        => subtype is AlterTableType.AT_AlterColumnType or AlterTableType.AT_SetExpression;
#endif

    /// <summary>
    /// Finds the tables that use a composite type: those with a column of it and those created of it.
    /// </summary>
    private static List<uint> TablesUsingType(uint typeOid) => typeOid == 0 ? [] : [.. Elevated.Run(() => Worf.Read("""
        SELECT a.attrelid FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        WHERE a.atttypid = $1 AND NOT a.attisdropped AND c.relkind IN ('r', 'p')
        UNION
        SELECT oid FROM pg_catalog.pg_class WHERE reloftype = $1 AND relkind IN ('r', 'p')
        """, 0, SpiParameter.Create(typeOid))).Select(row => row.Get<uint>(0))];

    /// <summary>
    /// Adds the tables that the foreign keys of some tables, their inheritors and their partitions reference, which a
    /// statement that rebuilds those keys may have use any valid unique index of.
    /// </summary>
    private static void AddRebuiltReferences(List<uint> invalid, List<uint> rebuilt)
    {
        List<long> relids = [];
        foreach (uint table in rebuilt)
        {
            List* tree = NativeMethods.find_all_inheritors(table, (int)PgLockMode.None, null);
            for (int j = 0; tree is not null && j < tree->length; j++)
            {
                relids.Add(tree->elements[j].oid_value);
            }
        }

        if (relids.Count > 0)
        {
            invalid.AddRange(Elevated.Run(() => Worf.Read("""
                SELECT DISTINCT confrelid FROM pg_catalog.pg_constraint
                WHERE contype = 'f' AND conrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[])
                """, 0, SpiParameter.Create(relids.ToArray()))).Select(row => row.Get<uint>(0)));
        }
    }

    /// <summary>
    /// Adds the tables that the foreign keys of a constraint, or of a column's constraints, reference.
    /// </summary>
    private static void AddReferenced(List<uint> tables, Node* node)
    {
        if (node->type == NodeTag.T_Constraint && ((Constraint*)node)->contype == ConstrType.CONSTR_FOREIGN)
        {
            _ = AddTable(tables, ((Constraint*)node)->pktable);
        }
        else if (node->type == NodeTag.T_ColumnDef)
        {
            List* constraints = ((ColumnDef*)node)->constraints;
            for (int i = 0; constraints is not null && i < constraints->length; i++)
            {
                AddReferenced(tables, (Node*)constraints->elements[i].ptr_value);
            }
        }
    }

    /// <summary>
    /// Adds the partitions under a partitioned table, however deep.
    /// </summary>
    private static void AddPartitions(List<uint> tables, uint relid)
    {
        List* partitions = NativeMethods.find_all_inheritors(relid, (int)PgLockMode.None, null);
        for (int i = 1; partitions is not null && i < partitions->length; i++)
        {
            tables.Add(partitions->elements[i].oid_value);
        }
    }

    /// <summary>
    /// Adds a table named in a statement, which may not exist.
    /// </summary>
    /// <returns>The table, or zero.</returns>
    private static uint AddTable(List<uint> tables, RangeVar* relation)
    {
        uint relid = relation is null ? 0 : NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        if (relid != 0)
        {
            tables.Add(relid);
        }

        return relid;
    }

    /// <summary>
    /// Hides the indexes on this node alone of some tables from a statement, as not live or as not valid. They're locked
    /// as a rename would lock them, so that no concurrent build, reindex or drop changes their state meanwhile.
    /// </summary>
    private static void Hide(List<uint> tables, bool unlive)
    {
        if (tables.Count == 0)
        {
            return;
        }

        long[] relids = [.. tables.Distinct().Select(table => (long)table)];
        WorfRows indexes = Elevated.Run(() => Worf.Read("""
            SELECT i.indexrelid FROM worf.local_index l JOIN pg_catalog.pg_index i ON i.indexrelid = l.indexrelid
            WHERE i.indrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[])
            ORDER BY 1
            """, 0, SpiParameter.Create(relids)));
        if (indexes.Count == 0)
        {
            return;
        }

        foreach (WorfRow index in indexes)
        {
            NativeMethods.LockRelationOid(index.Get<uint>(0), (int)PgLockMode.ShareUpdateExclusive);
        }

        WorfRows result = Elevated.Run(() => Worf.Query(unlive
            ? """
                UPDATE pg_catalog.pg_index i SET indislive = false FROM worf.local_index l, pg_catalog.pg_class t
                WHERE i.indexrelid = l.indexrelid AND t.oid = i.indrelid AND i.indrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[]) AND i.indislive
                RETURNING i.indexrelid, i.indrelid, t.relfilenode
                """
            : """
                UPDATE pg_catalog.pg_index i SET indisvalid = false FROM worf.local_index l
                WHERE i.indexrelid = l.indexrelid AND i.indrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[]) AND i.indisvalid AND i.indislive
                RETURNING i.indexrelid, i.indrelid, 0::pg_catalog.oid
                """,
            SpiParameter.Create(relids)));
        foreach (WorfRow row in result)
        {
            s_hidden.Add((row.Get<uint>(0), row.Get<uint>(1), unlive, row.Get<uint>(2)));
            if (unlive)
            {
                _ = s_unliveTables.Add(row.Get<uint>(1));
            }

            NativeMethods.CacheInvalidateRelcacheByRelid(row.Get<uint>(1));
        }

        NativeMethods.CommandCounterIncrement();
    }

    /// <summary>
    /// Tracks indexes as on this node alone, other than worf's own and those of its tables' TOAST tables, which
    /// nothing replicates, nor any that no longer exists, so that no index that takes its OID later inherits its mark.
    /// </summary>
    private static void Track(List<uint> relids)
    {
        if (relids.Count > 0)
        {
            Write("""
                INSERT INTO worf.local_index
                    SELECT i.indexrelid FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
                    WHERE i.indexrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[])
                        AND t.relnamespace <> 'worf'::pg_catalog.regnamespace
                        AND NOT EXISTS (SELECT FROM pg_catalog.pg_class w WHERE w.reltoastrelid = t.oid AND w.relnamespace = 'worf'::pg_catalog.regnamespace)
                ON CONFLICT DO NOTHING
                """, SpiParameter.Create(relids.Select(relid => (long)relid).ToArray()));
        }
    }

    /// <summary>
    /// Writes worf's own table, whoever runs the statement, with a snapshot, which a statement that commits on its way
    /// may lack between its steps.
    /// </summary>
    private static void Write(string command, params SpiParameter[] parameters)
        => _ = DdlReplication.WithSnapshot(() => Elevated.Run(() =>
        {
            _ = Worf.Execute(command, parameters);
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
            s_rebuiltAs.Clear();
            s_droppingConcurrently.Clear();
            s_namedBefore.Clear();
            s_unliveTables.Clear();
            s_tracking = false;
            s_reindexing = false;
            s_isolating = false;
            s_hidingRebuilt = false;
            return;
        }

        if (xactEvent == XactEvent.XACT_EVENT_PRE_COMMIT)
        {
            Flush();
            FollowSwaps();
            ForgetDropped();
            s_rebuiltAs.Clear();
        }
    }

    /// <summary>
    /// Forgets the local indexes that a concurrent drop has dropped, in its last transaction, which drops them, rather
    /// than in those before.
    /// </summary>
    private static void ForgetDropped()
    {
        List<uint> dropped = s_droppingConcurrently.FindAll(relid => NativeMethods.get_rel_relkind(relid) == 0);
        if (dropped.Count > 0)
        {
            _ = s_droppingConcurrently.RemoveAll(dropped.Contains);
            Write("DELETE FROM worf.local_index WHERE indexrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[])",
                SpiParameter.Create(dropped.Select(relid => (long)relid).ToArray()));
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
