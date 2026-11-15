using System.Globalization;
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

    // Whether the indexes on this node alone are out of a statement's way, and those hidden from it, and whether those
    // it builds again are hidden too.
    private static bool s_isolating;
    private static bool s_hidingRebuilt;
    private static List<(uint Index, uint Table, bool Unlive, uint Filenode)> s_hidden = [];

    // Whether the names PostgreSQL chooses for the indexes and sequences a statement creates are made those the other
    // nodes choose, which no index on this node alone holds there, once it has run; the relations it creates, the
    // names it gives, and those of the relations it drops, as one an index built again takes.
    private static bool s_naming;
    private static readonly List<uint> s_named = [];
    private static readonly HashSet<string> s_givenNames = [];
    private static readonly HashSet<(uint Schema, string Name)> s_droppedNames = [];

    // The tables whose indexes a statement copies with LIKE ... INCLUDING INDEXES, which have indexes on this node alone,
    // with whether it copies their comments, and the indexes on this node alone whose copies it left out.
    private static readonly List<(uint Table, bool Comments)> s_copied = [];
    private static readonly HashSet<uint> s_copiesLeftOut = [];

    // The tables whose indexes on this node alone a statement hides as not live, which nothing may write to meanwhile.
    private static readonly HashSet<uint> s_unliveTables = [];

    // RVR_MISSING_OK from catalog/namespace.h, and CREATE_TABLE_LIKE_INDEXES and CREATE_TABLE_LIKE_COMMENTS from
    // nodes/parsenodes.h.
    private const uint RangeVarMissingOk = 1;
    private const uint CreateTableLikeIndexes = 1 << 6;
    private const uint CreateTableLikeComments = 1 << 0;
    private const uint RelationRelationId = 1259;

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
    /// Gets or sets the node whose DDL apply runs, for the warning when it takes the name of an index on this node alone,
    /// or null.
    /// </summary>
    internal static string? Origin { get; set; }

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
        if (s_naming)
        {
            s_named.Add(relid);
        }

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
        sbyte* droppedName = s_naming ? NativeMethods.get_rel_name(relid) : null;
        if (droppedName is not null)
        {
            _ = s_droppedNames.Add((NativeMethods.get_rel_namespace(relid), Worf.FromServer(droppedName)));
        }

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
    /// Finds the names a statement gives relations, which an index on this node alone could have: those it gives
    /// relations it creates or renames, or that a table, index or type it moves to another schema has, with the
    /// indexes and sequences that move with it. A statement that can create relations anywhere, as a DO block, a
    /// procedure or an extension can, gives none itself, as its own statements give theirs, and neither does any other,
    /// as a DROP or a statement that names no relation.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <param name="pathKnown">Whether the search path the statement finds names on is the current one.</param>
    /// <returns>The names, in their schemas or, with a zero schema, in any, or null when it can't be told.</returns>
    private static List<(uint Schema, string Name)>? NamesTaken(Node* parsetree, bool pathKnown)
    {
        List<(uint Schema, string Name)> names = [];
        switch (parsetree->type)
        {
            // EXPLAIN ANALYZE runs the statement it explains, which parse analysis has made a query, as it has a utility
            // statement that EXPLAIN runs, as CREATE TABLE AS.
            case NodeTag.T_ExplainStmt:
                return NamesTaken(((ExplainStmt*)parsetree)->query, pathKnown);

            case NodeTag.T_Query when ((Query*)parsetree)->utilityStmt is not null:
                return NamesTaken(((Query*)parsetree)->utilityStmt, pathKnown);

            // A table created from a query, as by CREATE TABLE AS or SELECT INTO, takes the name it gives. The relations
            // that the functions a statement runs create, as a DO block's, a procedure's, an extension's script's or
            // a query's do, are created by statements of their own, which give their own names.
            case NodeTag.T_CreateTableAsStmt when ((CreateTableAsStmt*)parsetree)->into is not null:
                RangeVar* into = ((CreateTableAsStmt*)parsetree)->into->rel;
                names.Add((SchemaOf(into, true, pathKnown), Worf.FromServer(into->relname)));
                break;

            case NodeTag.T_CreateStmt or NodeTag.T_CreateForeignTableStmt:
                CreateStmt* create = (CreateStmt*)parsetree;
                uint schema = SchemaOf(create->relation, true, pathKnown);
                names.Add((schema, Worf.FromServer(create->relation->relname)));
                AddConstraintNames(names, schema, create->tableElts);
                break;

            case NodeTag.T_IndexStmt:
                IndexStmt* index = (IndexStmt*)parsetree;
                if (index->idxname is not null)
                {
                    names.Add((SchemaOf(index->relation, false, pathKnown), Worf.FromServer(index->idxname)));
                }

                break;

            case NodeTag.T_AlterTableStmt when ((AlterTableStmt*)parsetree)->objtype is ObjectType.OBJECT_TABLE or ObjectType.OBJECT_FOREIGN_TABLE:
                AlterTableStmt* alter = (AlterTableStmt*)parsetree;
                uint altered = SchemaOf(alter->relation, false, pathKnown);
                List* cmds = alter->cmds;
                for (int i = 0; cmds is not null && i < cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
                    if (cmd->subtype is AlterTableType.AT_AddConstraint or AlterTableType.AT_AddColumn or AlterTableType.AT_AddIdentity &&
                        cmd->def is not null)
                    {
                        AddConstraintNames(names, altered, cmd->def);
                    }
                    else if (cmd->subtype is AlterTableType.AT_AddIndex or AlterTableType.AT_AddIndexConstraint && cmd->def is not null &&
                        ((IndexStmt*)cmd->def)->idxname is not null)
                    {
                        names.Add((altered, Worf.FromServer(((IndexStmt*)cmd->def)->idxname)));
                    }
                }

                break;

            case NodeTag.T_CreateSeqStmt:
                names.Add((SchemaOf(((CreateSeqStmt*)parsetree)->sequence, true, pathKnown), Worf.FromServer(((CreateSeqStmt*)parsetree)->sequence->relname)));
                break;

            case NodeTag.T_ViewStmt:
                names.Add((SchemaOf(((ViewStmt*)parsetree)->view, true, pathKnown), Worf.FromServer(((ViewStmt*)parsetree)->view->relname)));
                break;

            case NodeTag.T_CompositeTypeStmt:
                names.Add((SchemaOf(((CompositeTypeStmt*)parsetree)->typevar, true, pathKnown), Worf.FromServer(((CompositeTypeStmt*)parsetree)->typevar->relname)));
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
    private static List<(uint Schema, string Name)>? RenamedNames(RenameStmt* rename, bool pathKnown)
    {
        string newname = rename->newname is null ? "" : Worf.FromServer(rename->newname);
        switch (rename->renameType)
        {
            case ObjectType.OBJECT_TABLE or ObjectType.OBJECT_INDEX or ObjectType.OBJECT_SEQUENCE or ObjectType.OBJECT_VIEW or
                ObjectType.OBJECT_MATVIEW or ObjectType.OBJECT_FOREIGN_TABLE or ObjectType.OBJECT_TABCONSTRAINT:
                return [(SchemaOf(rename->relation, false, pathKnown), newname)];

            case ObjectType.OBJECT_TYPE:
                uint typid = NativeMethods.LookupTypeNameOid(null, NativeMethods.makeTypeNameFromNameList((List*)rename->@object), true);
                uint typrelid = typid == 0 ? 0 : NativeMethods.get_typ_typrelid(typid);
                return typid == 0 ? null : typrelid == 0 ? [] : [(pathKnown ? NativeMethods.get_rel_namespace(typrelid) : 0, newname)];

            default:
                return [];
        }
    }

    /// <summary>
    /// Finds the names that a relation, a composite type's relation or an extension's relations moved to another
    /// schema take there, with those of the indexes and sequences that move with them.
    /// </summary>
    private static List<(uint Schema, string Name)>? MovedNames(AlterObjectSchemaStmt* move, bool pathKnown)
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
                    """, 0, SpiParameter.Create(extension.objectId))).Select(row => (target, row.Get<string>(0)))];

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
            """, 0, SpiParameter.Create(relid))).Select(row => (target, row.Get<string>(0)))];
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
    private static void AddConstraintNames(List<(uint Schema, string Name)> names, uint schema, List* elements)
    {
        for (int i = 0; elements is not null && i < elements->length; i++)
        {
            AddConstraintNames(names, schema, (Node*)elements->elements[i].ptr_value);
        }
    }

    private static void AddConstraintNames(List<(uint Schema, string Name)> names, uint schema, Node* element)
    {
        if (element->type == NodeTag.T_ColumnDef)
        {
            AddConstraintNames(names, schema, ((ColumnDef*)element)->constraints);
        }
        else if (element->type == NodeTag.T_Constraint && ((Constraint*)element)->conname is not null &&
            ((Constraint*)element)->contype is ConstrType.CONSTR_PRIMARY or ConstrType.CONSTR_UNIQUE or ConstrType.CONSTR_EXCLUSION)
        {
            names.Add((schema, Worf.FromServer(((Constraint*)element)->conname)));
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
                    names.Add((nspname is null ? schema : NativeMethods.get_namespace_oid(nspname, true), Worf.FromServer(objname)));
                }
            }
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
    /// Keeps the indexes on this node alone out of the way of DDL that replicates, or DDL from another node, and of DDL
    /// that a function it calls runs, whose work joins the statement's, which alone ends it; the commands PostgreSQL
    /// runs for a statement, with names it chose, are the statement's own: one that has a name the DDL gives a relation
    /// is renamed, as
    /// <see cref="RenameTakenLocals"/> renames it, and once the DDL has run, the names PostgreSQL chose beside them are
    /// made those the other nodes choose, as <see cref="Reconcile"/> makes them; a copy of a table's indexes leaves them
    /// out, as <see cref="SkipsCopy"/> does; and they're hidden from DDL that would attach them to a partitioned index
    /// or have a foreign key use them, none of which the other nodes can. Only the indexes of the tables the statement
    /// attaches, indexes the partitions of or references are hidden: those a foreign key or an attached partition would
    /// use are marked not valid, which they're still kept up to date and built again as, and those that a partitioned
    /// index would take, which only indexes that aren't live escape, are marked not live, while nothing may write to or
    /// rewrite their tables. ALTER TABLE that names no index hides those it builds again as not valid as it creates
    /// them.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <param name="replicated">Whether it's DDL to replicate.</param>
    /// <param name="subcommand">Whether PostgreSQL runs it for another statement.</param>
    /// <returns>Whether any are out of its way, for <see cref="EndIsolation"/> and <see cref="Restore"/>.</returns>
    internal static bool Isolate(Node* parsetree, bool replicated, bool subcommand)
    {
        if (s_isolating && subcommand)
        {
            return false;
        }

        bool rename = (replicated || Applying) && ChoosesNames(parsetree);
        bool hidingRebuilt = parsetree->type == NodeTag.T_AlterTableStmt && (Applying || rename);
        List<(uint Table, IndexShape Shape, PgLockMode Lock)> unlive = [];
        List<(uint Table, string[]? Columns, PgLockMode Lock)> invalid = [];
        if (replicated || Applying)
        {
            UsedTables(parsetree, unlive, invalid);
        }

        List<(uint Table, bool Comments)> copied = replicated || Applying ? CopiedTables(parsetree) : [];
        List<(uint Schema, string Name)>? given = replicated || Applying ? NamesTaken(parsetree, true) : null;
        if (!(rename || given is { Count: > 0 } || hidingRebuilt || unlive.Count > 0 || invalid.Count > 0 || copied.Count > 0) ||
            Worf.CatalogOid("local_index") == 0 || !Elevated.Run(() => Worf.ReadScalar<bool>("SELECT EXISTS (SELECT FROM worf.local_index)")))
        {
            return false;
        }

        // A statement that one being isolated runs adds what it needs to that statement's isolation, which ends it.
        bool nested = s_isolating;
        if (!nested)
        {
            s_named.Clear();
            s_droppedNames.Clear();
            s_givenNames.Clear();
            s_hidden = [];
        }

        s_copied.AddRange(copied);
        RenameTakenLocals(given);
        s_naming |= rename;
        foreach ((uint _, string name) in given ?? [])
        {
            _ = s_givenNames.Add(name);
        }

        HideUnlive(unlive);
        HideInvalid(invalid);
        s_hidingRebuilt |= hidingRebuilt;
        s_isolating = true;
        return !nested;
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
            s_copied.Clear();
            s_copiesLeftOut.Clear();
            s_naming = false;
        }
    }

    /// <summary>
    /// Brings the indexes on this node alone back once a statement that <see cref="Isolate"/> ran out of their way has
    /// run: live and valid again, with the names PostgreSQL chose for what it created made those the other nodes chose,
    /// as <see cref="Reconcile"/> makes them. A statement that
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
        s_hidden = [];
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
        Reconcile();
    }

    /// <summary>
    /// Renames each index on this node alone that has a name DDL that replicates, or DDL from another node, gives a
    /// relation it creates, renames or moves, which the other nodes don't have, to a name of its own, with a warning,
    /// so that the DDL does here what it does on them. Only an index that has such a name is renamed, as the rename
    /// holds it until the transaction ends.
    /// </summary>
    /// <param name="given">The names, in their schemas or, with a zero schema, in any.</param>
    private static void RenameTakenLocals(List<(uint Schema, string Name)>? given)
    {
        if (given is not { Count: > 0 })
        {
            return;
        }

        foreach ((uint schema, string name) in given)
        {
            WorfRows holders = Elevated.Run(() => Worf.Read("""
                SELECT c.oid FROM worf.local_index l JOIN pg_catalog.pg_class c ON c.oid = l.indexrelid
                WHERE c.relname = $1 AND ($2 = 0 OR c.relnamespace = $2) ORDER BY 1
                """, 0, Worf.TextParameter(name), SpiParameter.Create(schema)));
            foreach (uint holder in holders.Select(row => row.Get<uint>(0)))
            {
                RenameLocalAway(holder);
            }
        }
    }

    /// <summary>
    /// Renames an index on this node alone whose name DDL takes to a name of its own, with a warning.
    /// </summary>
    private static void RenameLocalAway(uint index)
    {
        sbyte* name = NativeMethods.get_rel_name(index);
        uint nspoid = NativeMethods.get_rel_namespace(index);
        sbyte* target = NativeMethods.ChooseRelationName(name, null, Worf.PStrDup("local"), nspoid, false);
        WorfLog.Warning($"renamed index \"{Worf.FromServer(name)}\", which is on this node alone, to \"{Worf.FromServer(target)}\", as DDL{(Origin is null ? "" : $" from node {Origin}")} took its name");
        NativeMethods.RenameRelationInternal(index, target, true, true);
        NativeMethods.CommandCounterIncrement();
    }

    /// <summary>
    /// Gives the indexes and sequences a statement created, under names PostgreSQL chose beside the indexes on this
    /// node alone, the names it chooses without them, as on the other nodes: the first of the names
    /// <c>ChooseRelationName</c> tries that no other relation, nor for a constraint's index another constraint, has,
    /// in the order the statement created them. An index on this node alone that has the name is renamed to a name of
    /// its own. A relation the statement named itself, or that took the name of one it dropped, as an index built
    /// again does, keeps its name.
    /// </summary>
    private static void Reconcile()
    {
        List<uint> named = [.. s_named.Distinct().Order()];
        s_named.Clear();
        foreach (uint relid in named)
        {
            sbyte kind = NativeMethods.get_rel_relkind(relid);
            sbyte* current = kind is (sbyte)'i' or (sbyte)'I' or (sbyte)'S' ? NativeMethods.get_rel_name(relid) : null;
            if (current is null || Contains(relid) || s_givenNames.Contains(Worf.FromServer(current)))
            {
                continue;
            }

            uint nspoid = NativeMethods.get_rel_namespace(relid);
            if (s_droppedNames.Contains((nspoid, Worf.FromServer(current))) || Naming(relid, kind) is not { } naming)
            {
                continue;
            }

            byte[] target = Chosen(relid, naming.Name1, naming.Name2, naming.Label, nspoid, naming.Constraint);
            if (target.AsSpan().SequenceEqual(new ReadOnlySpan<byte>(current, Worf.StrLen(current))))
            {
                continue;
            }

            fixed (byte* targetName = target)
            {
                uint holder = NativeMethods.get_relname_relid((sbyte*)targetName, nspoid);
                if (holder != 0 && holder != relid && Contains(holder))
                {
                    RenameLocalAway(holder);
                }

                NativeMethods.RenameRelationInternal(relid, (sbyte*)targetName, true, kind is (sbyte)'i' or (sbyte)'I');
            }

            NativeMethods.CommandCounterIncrement();
        }

        s_droppedNames.Clear();
        s_givenNames.Clear();
    }

    /// <summary>
    /// Finds what PostgreSQL chose a created index's or sequence's name from: for an index, as <c>ChooseIndexName</c>
    /// does, its table's name, its columns' names, joined as <c>ChooseIndexNameAddition</c> joins them, and a label
    /// for what it is; for a sequence a column owns, as for a serial or identity column, the table's and the column's
    /// names.
    /// </summary>
    /// <returns>The names, label and whether the index backs a constraint, or null for a sequence no column owns.</returns>
    private static (byte[] Name1, byte[]? Name2, string Label, bool Constraint)? Naming(uint relid, sbyte kind)
    {
        if (kind == (sbyte)'S')
        {
            WorfRows owner = Elevated.Run(() => Worf.Read("""
                SELECT c.relname::pg_catalog.text, a.attname::pg_catalog.text FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_class c ON c.oid = d.refobjid
                    JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
                WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = $1
                    AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.deptype IN ('a', 'i')
                """, 1, SpiParameter.Create(relid)));
            return owner.Count == 0 ? null : (Worf.ToServer(owner[0].Get<string>(0)), Worf.ToServer(owner[0].Get<string>(1)), "seq", false);
        }

        WorfRows index = Elevated.Run(() => Worf.Read("""
            SELECT t.relname::pg_catalog.text, i.indisprimary, i.indisexclusion
            FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class t ON t.oid = i.indrelid WHERE i.indexrelid = $1
            """, 1, SpiParameter.Create(relid)));
        if (index.Count == 0)
        {
            return null;
        }

        byte[] table = Worf.ToServer(index[0].Get<string>(0));
        if (index[0].Get<bool>(1))
        {
            return (table, null, "pkey", true);
        }

        bool constraint = NativeMethods.get_index_constraint(relid) != 0;
        WorfRows columns = Elevated.Run(() => Worf.Read(
            "SELECT attname::pg_catalog.text FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 ORDER BY attnum", 0, SpiParameter.Create(relid)));
        byte[] addition = Addition([.. columns.Select(column => Worf.ToServer(column.Get<string>(0)))]);
        return index[0].Get<bool>(2) ? (table, addition, "excl", true) : constraint ? (table, addition, "key", true) : (table, addition, "idx", false);
    }

    /// <summary>
    /// Joins an index's column names with underscores, as <c>ChooseIndexNameAddition</c> does: each at most
    /// NAMEDATALEN - 1 bytes, until the join has NAMEDATALEN bytes or more.
    /// </summary>
    private static byte[] Addition(List<byte[]> names)
    {
        List<byte> joined = [];
        foreach (byte[] name in names)
        {
            if (joined.Count > 0)
            {
                joined.Add((byte)'_');
            }

            joined.AddRange(name.AsSpan(0, Math.Min(name.Length, Worker.NameDataLen - 1)).ToArray());
            if (joined.Count >= Worker.NameDataLen)
            {
                break;
            }
        }

        return [.. joined];
    }

    /// <summary>
    /// Chooses a relation's name as <c>ChooseRelationName</c> does, but as though the indexes on this node alone, and the
    /// relation itself, weren't there.
    /// </summary>
    private static byte[] Chosen(uint relid, byte[] name1, byte[]? name2, string label, uint nspoid, bool constraint)
    {
        fixed (byte* first = Terminated(name1))
        fixed (byte* second = name2 is null ? null : Terminated(name2))
        {
            for (int pass = 0; ; pass++)
            {
                byte[] modlabel = Worf.ToServerCString(pass == 0 ? label : label + pass.ToString(CultureInfo.InvariantCulture));
                fixed (byte* labelAddress = modlabel)
                {
                    sbyte* candidate = NativeMethods.makeObjectName((sbyte*)first, (sbyte*)second, (sbyte*)labelAddress);
                    uint holder = NativeMethods.get_relname_relid(candidate, nspoid);
                    bool taken = holder != 0 && holder != relid && !Contains(holder);
                    if (!taken && constraint && holder != relid && NativeMethods.ConstraintNameExists(candidate, nspoid))
                    {
                        taken = true;
                    }

                    if (!taken)
                    {
                        return new ReadOnlySpan<byte>(candidate, Worf.StrLen(candidate)).ToArray();
                    }
                }
            }
        }
    }

    private static byte[] Terminated(byte[] name) => [.. name, 0];

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
    /// <param name="origin">The node the DDL came from.</param>
    /// <returns>Whether the index was dropped, so that the DDL can run again.</returns>
    internal static bool DropUnbuilt(PgException error, string origin)
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

        string name = error.ConstraintName!;
        string definition = Worf.FromServer(NativeMethods.pg_get_indexdef_string(relid));

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
    /// Finds the tables that a <c>CREATE TABLE</c> copies the indexes of with <c>LIKE ... INCLUDING INDEXES</c>, with
    /// whether it copies their comments.
    /// </summary>
    private static List<(uint Table, bool Comments)> CopiedTables(Node* parsetree)
    {
        List<(uint Table, bool Comments)> copied = [];
        if (parsetree->type != NodeTag.T_CreateStmt)
        {
            return copied;
        }

        List* elements = ((CreateStmt*)parsetree)->tableElts;
        for (int i = 0; elements is not null && i < elements->length; i++)
        {
            Node* element = (Node*)elements->elements[i].ptr_value;
            if (element->type == NodeTag.T_TableLikeClause && (((TableLikeClause*)element)->options & CreateTableLikeIndexes) != 0)
            {
                uint table = NativeMethods.RangeVarGetRelidExtended(((TableLikeClause*)element)->relation, 0, RangeVarMissingOk, default, null);
                if (table != 0)
                {
                    copied.Add((table, (((TableLikeClause*)element)->options & CreateTableLikeComments) != 0));
                }
            }
        }

        return copied;
    }

    /// <summary>
    /// Checks whether an index that <c>CREATE TABLE ... LIKE ... INCLUDING INDEXES</c> is about to build copies one of
    /// the copied table's indexes on this node alone, which the other nodes don't copy, and which it then leaves out,
    /// so that it builds the indexes and chooses the names that they do, without hiding the indexes on this node alone,
    /// which would hold them until the transaction ends. PostgreSQL builds each copy as a statement of its own, made by
    /// <c>generateClonedIndexStmt</c>, which this compares with the copy it makes of each of those indexes: one of
    /// them is never a constraint, as a statement for the new table's own constraints is, and a copy of another index
    /// that's the same builds the same index, under the same name, as the one left out would have.
    /// </summary>
    /// <param name="stmt">The index's statement, which runs as a part of the <c>CREATE TABLE</c>.</param>
    /// <returns>Whether the index is left out.</returns>
    internal static bool SkipsCopy(IndexStmt* stmt)
    {
        if (s_copied.Count == 0 || stmt->relation is null)
        {
            return false;
        }

        uint created = NativeMethods.RangeVarGetRelidExtended(stmt->relation, 0, RangeVarMissingOk, default, null);
        if (created == 0)
        {
            return false;
        }

        foreach ((uint table, bool comments) in s_copied)
        {
            WorfRows locals = Elevated.Run(() => Worf.Read("""
                SELECT l.indexrelid FROM worf.local_index l JOIN pg_catalog.pg_index i ON i.indexrelid = l.indexrelid
                WHERE i.indrelid = $1 ORDER BY 1
                """, 0, SpiParameter.Create(table)));
            foreach (uint index in locals.Select(row => row.Get<uint>(0)).Where(index => !s_copiesLeftOut.Contains(index)))
            {
                if (CopiesIndex(stmt, created, table, index, comments))
                {
                    _ = s_copiesLeftOut.Add(index);
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Checks whether an index's statement is the copy that <c>expandTableLikeClause</c> makes of an index.
    /// </summary>
    private static bool CopiesIndex(IndexStmt* stmt, uint created, uint table, uint index, bool comments)
    {
        RelationData* createdRel = NativeMethods.relation_open(created, (int)PgLockMode.None);
        RelationData* tableRel = NativeMethods.relation_open(table, (int)PgLockMode.None);
        RelationData* indexRel = NativeMethods.index_open(index, (int)PgLockMode.AccessShare);
        try
        {
#if ANKUS_PG15
            AttrMap* attmap = NativeMethods.build_attrmap_by_name(createdRel->rd_att, tableRel->rd_att);
#else
            AttrMap* attmap = NativeMethods.build_attrmap_by_name(createdRel->rd_att, tableRel->rd_att, false);
#endif
            IndexStmt* copy = NativeMethods.generateClonedIndexStmt(stmt->relation, indexRel, attmap, null);
            if (comments)
            {
                copy->idxcomment = NativeMethods.GetComment(index, RelationRelationId, 0);
            }

            NativeMethods.free_attrmap(attmap);
            return NativeMethods.equal(copy, stmt);
        }
        finally
        {
            NativeMethods.index_close(indexRel, (int)PgLockMode.None);
            NativeMethods.relation_close(tableRel, (int)PgLockMode.None);
            NativeMethods.relation_close(createdRel, (int)PgLockMode.None);
        }
    }

    /// <summary>
    /// Finds the indexes on this node alone that a statement would attach to a partitioned index, or have a foreign key
    /// use, by their tables and the columns those take: the partitions of a partitioned table indexed, whose index by
    /// the same columns the index may take, and those of a partition attached, which the partitioned table's indexes
    /// may take, and for a foreign key, the valid unique index by the columns it references, of the table it
    /// references, or of the partition attached, which the keys that reference the partitioned table use. A copy of a
    /// table's indexes leaves out its indexes on this node alone as <see cref="SkipsCopy"/> has it, and a foreign key
    /// that references a primary key uses no other index.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <param name="unlive">The tables whose indexes by the columns given, or by any for null, are to be hidden as not
    /// live.</param>
    /// <param name="invalid">The tables whose valid unique indexes by the columns given, or by any for null, are to be
    /// hidden as not valid.</param>
    private static void UsedTables(Node* parsetree, List<(uint Table, IndexShape Shape, PgLockMode Lock)> unlive, List<(uint Table, string[]? Columns, PgLockMode Lock)> invalid)
    {
        switch (parsetree->type)
        {
            case NodeTag.T_CreateStmt:
                List* elements = ((CreateStmt*)parsetree)->tableElts;
                for (int i = 0; elements is not null && i < elements->length; i++)
                {
                    AddReferenced(invalid, (Node*)elements->elements[i].ptr_value);
                }

                break;

            case NodeTag.T_IndexStmt when !((IndexStmt*)parsetree)->concurrent && ((IndexStmt*)parsetree)->relation->inh:
                IndexStmt* index = (IndexStmt*)parsetree;
                uint indexed = NativeMethods.RangeVarGetRelidExtended(index->relation, 0, RangeVarMissingOk, default, null);
                if (indexed != 0 && NativeMethods.get_rel_relkind(indexed) == (sbyte)'p' && index->excludeOpNames is null)
                {
                    IndexShape shape = new(index->unique, index->nulls_not_distinct, Worf.FromServer(index->accessMethod),
                        [.. IndexColumns(index->indexParams), .. IndexColumns(index->indexIncludingParams)], ListLength(index->indexParams),
                        index->whereClause is not null, false);
                    foreach (uint partition in Partitions(indexed))
                    {
                        unlive.Add((partition, shape, PgLockMode.Share));
                    }
                }

                break;

            case NodeTag.T_AlterTableStmt when ((AlterTableStmt*)parsetree)->objtype is ObjectType.OBJECT_TABLE or ObjectType.OBJECT_TYPE:
                bool type = ((AlterTableStmt*)parsetree)->objtype == ObjectType.OBJECT_TYPE;
                uint altered = NativeMethods.RangeVarGetRelidExtended(((AlterTableStmt*)parsetree)->relation, 0, RangeVarMissingOk, default, null);
                List* cmds = ((AlterTableStmt*)parsetree)->cmds;
                for (int i = 0; cmds is not null && i < cmds->length; i++)
                {
                    AlterTableCmd* cmd = (AlterTableCmd*)cmds->elements[i].ptr_value;
                    if (!type && cmd->subtype == AlterTableType.AT_AttachPartition && cmd->def is not null && altered != 0)
                    {
                        uint attached = NativeMethods.RangeVarGetRelidExtended(((PartitionCmd*)cmd->def)->name, 0, RangeVarMissingOk, default, null);
                        if (attached != 0)
                        {
                            AddAttached(unlive, invalid, altered, attached);
                        }
                    }
                    else if (!type && cmd->subtype is AlterTableType.AT_AddConstraint or AlterTableType.AT_AddColumn && cmd->def is not null)
                    {
                        AddReferenced(invalid, cmd->def);
                    }
                    else if (Rewrites(cmd->subtype) && altered != 0)
                    {
                        // A column's new type or expression builds the table's foreign keys again, and those of its
                        // inheritors and partitions, which may then use any valid unique index by their columns on the
                        // tables they reference. An attribute's new type in a composite type does so for every table that
                        // uses the type, which the change cascades to.
                        AddRebuiltReferences(invalid, type ? TablesUsingType(NativeMethods.get_rel_type_id(altered)) : [altered]);
                    }
                }

                break;

            default:
                break;
        }
    }

    /// <summary>
    /// Finds the columns of an index's elements by name, with an empty one for an expression.
    /// </summary>
    private static List<string> IndexColumns(List* elements)
    {
        List<string> columns = [];
        for (int i = 0; elements is not null && i < elements->length; i++)
        {
            IndexElem* element = (IndexElem*)elements->elements[i].ptr_value;
            columns.Add(element->name is null ? "" : Worf.FromServer(element->name));
        }

        return columns;
    }

    private static int ListLength(List* list) => list is null ? 0 : list->length;

    /// <summary>
    /// Reads the shapes of a table's indexes, or of those on this node alone, by which a partitioned index may take them.
    /// </summary>
    /// <param name="table">The table.</param>
    /// <param name="local">Whether only those on this node alone.</param>
    /// <returns>Each index and its shape.</returns>
    private static List<(uint Index, IndexShape Shape)> IndexShapes(uint table, bool local)
    {
        WorfRows indexes = Elevated.Run(() => Worf.Read("""
            SELECT i.indexrelid, i.indisunique, i.indnullsnotdistinct, m.amname::pg_catalog.text,
                array_agg(COALESCE(a.attname::pg_catalog.text, '') ORDER BY k.n), i.indnkeyatts::pg_catalog.int4, i.indpred IS NOT NULL, i.indisexclusion
            FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid JOIN pg_catalog.pg_am m ON m.oid = c.relam
                CROSS JOIN LATERAL pg_catalog.unnest(i.indkey::pg_catalog.int2[]) WITH ORDINALITY AS k (attnum, n)
                LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum AND k.attnum > 0
            WHERE i.indrelid = $1 AND (NOT $2 OR i.indexrelid IN (SELECT indexrelid FROM worf.local_index))
            GROUP BY i.indexrelid, i.indisunique, i.indnullsnotdistinct, m.amname, i.indnkeyatts, i.indpred, i.indisexclusion
            """, 0, SpiParameter.Create(table), SpiParameter.Create(local)));
        return [.. indexes.Select(row => (row.Get<uint>(0), new IndexShape(row.Get<bool>(1), row.Get<bool>(2), row.Get<string>(3),
            row.Get<string[]>(4), row.Get<int>(5), row.Get<bool>(6), row.Get<bool>(7))))];
    }

    /// <summary>
    /// Adds what attaching a partition may use: the attached table's and its partitions' indexes by the columns of each
    /// of the partitioned table's indexes, which it takes for them, and the attached table's valid unique ones by the
    /// columns of each foreign key that references the partitioned table, which then uses them.
    /// </summary>
    private static void AddAttached(List<(uint Table, IndexShape Shape, PgLockMode Lock)> unlive, List<(uint Table, string[]? Columns, PgLockMode Lock)> invalid, uint parent, uint attached)
    {
        List<uint> tables = [attached, .. Partitions(attached)];
        foreach ((uint _, IndexShape shape) in IndexShapes(parent, false))
        {
            foreach (uint table in tables)
            {
                unlive.Add((table, shape, PgLockMode.AccessExclusive));
            }
        }

        foreach (WorfRow key in Elevated.Run(() => Worf.Read("""
            SELECT array_agg(a.attname::pg_catalog.text) FROM pg_catalog.pg_constraint c
                CROSS JOIN LATERAL pg_catalog.unnest(c.confkey) AS k (attnum)
                JOIN pg_catalog.pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum
            WHERE c.contype = 'f' AND c.confrelid = $1
            GROUP BY c.oid
            """, 0, SpiParameter.Create(parent))))
        {
            invalid.Add((attached, key.Get<string[]>(0), PgLockMode.AccessExclusive));
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
    /// Adds the tables, with the columns, that the foreign keys of some tables, their inheritors and their partitions
    /// reference, which a statement that rebuilds those keys may have use any valid unique index by those columns of.
    /// </summary>
    private static void AddRebuiltReferences(List<(uint Table, string[]? Columns, PgLockMode Lock)> invalid, List<uint> rebuilt)
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
                SELECT c.confrelid, array_agg(a.attname::pg_catalog.text) FROM pg_catalog.pg_constraint c
                    CROSS JOIN LATERAL pg_catalog.unnest(c.confkey) AS k (attnum)
                    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum
                WHERE c.contype = 'f' AND c.conrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[])
                GROUP BY c.oid, c.confrelid
                """, 0, SpiParameter.Create(relids.ToArray()))).Select(row => (row.Get<uint>(0), row.Get<string[]?>(1), PgLockMode.ShareRowExclusive)));
        }
    }

    /// <summary>
    /// Adds the table, with the columns, that a foreign key, or a column's foreign keys, reference by name. One that
    /// references the table's primary key uses no other index.
    /// </summary>
    private static void AddReferenced(List<(uint Table, string[]? Columns, PgLockMode Lock)> tables, Node* node)
    {
        if (node->type == NodeTag.T_Constraint && ((Constraint*)node)->contype == ConstrType.CONSTR_FOREIGN)
        {
            Constraint* key = (Constraint*)node;
            uint table = key->pktable is null ? 0 : NativeMethods.RangeVarGetRelidExtended(key->pktable, 0, RangeVarMissingOk, default, null);
            List<string> columns = [];
            for (int i = 0; key->pk_attrs is not null && i < key->pk_attrs->length; i++)
            {
                columns.Add(Worf.FromServer(NativeMethods.NameListToString(NativeMethods.lappend(null, key->pk_attrs->elements[i].ptr_value))));
            }

            if (table != 0 && columns.Count > 0)
            {
                tables.Add((table, [.. columns], PgLockMode.ShareRowExclusive));
            }
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
    /// Finds the partitions under a partitioned table, however deep.
    /// </summary>
    private static List<uint> Partitions(uint relid)
    {
        List<uint> partitions = [];
        List* tree = NativeMethods.find_all_inheritors(relid, (int)PgLockMode.None, null);
        for (int i = 1; tree is not null && i < tree->length; i++)
        {
            partitions.Add(tree->elements[i].oid_value);
        }

        return partitions;
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
    /// Hides the indexes on this node alone that a partitioned index a statement creates, or a partition it attaches,
    /// may take for its own, as not live, those whose shape PostgreSQL compares alike, as <see cref="Hide"/> hides them.
    /// </summary>
    private static void HideUnlive(List<(uint Table, IndexShape Shape, PgLockMode Lock)> requests)
    {
        HashSet<uint> matching = [];
        foreach ((uint table, IndexShape shape, PgLockMode _) in requests)
        {
            foreach ((uint index, IndexShape candidate) in IndexShapes(table, true))
            {
                if (candidate.MayMatch(shape))
                {
                    _ = matching.Add(index);
                }
            }
        }

        Hide([.. requests.Select(request => (request.Table, request.Lock))], matching, true);
    }

    /// <summary>
    /// Hides the indexes on this node alone that a foreign key a statement adds or builds again may use, as not valid,
    /// as <see cref="Hide"/> hides them: an immediate valid unique index without expressions or a predicate, by its
    /// columns as a set, or any such for no columns given.
    /// </summary>
    private static void HideInvalid(List<(uint Table, string[]? Columns, PgLockMode Lock)> requests)
    {
        HashSet<uint> matching = [];
        foreach ((uint table, string[]? columns, PgLockMode _) in requests)
        {
            WorfRows candidates = Elevated.Run(() => Worf.Read("""
                SELECT i.indexrelid, array_agg(a.attname::pg_catalog.text ORDER BY k.n) FILTER (WHERE a.attname IS NOT NULL),
                    bool_or(k.attnum = 0) OR i.indexprs IS NOT NULL, i.indisunique AND i.indimmediate AND i.indisvalid AND i.indpred IS NULL
                FROM worf.local_index l JOIN pg_catalog.pg_index i ON i.indexrelid = l.indexrelid
                    CROSS JOIN LATERAL pg_catalog.unnest(i.indkey::pg_catalog.int2[]) WITH ORDINALITY AS k (attnum, n)
                    LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                WHERE i.indrelid = $1 AND k.n <= i.indnkeyatts
                GROUP BY i.indexrelid, i.indexprs, i.indisunique, i.indimmediate, i.indisvalid, i.indpred
                """, 0, SpiParameter.Create(table)));
            foreach (WorfRow candidate in candidates)
            {
                string[] keys = candidate.Get<string[]?>(1) ?? [];
                bool byColumns = columns is null || (keys.Length == columns.Length && !keys.Except(columns).Any());
                if (candidate.Get<bool>(3) && !candidate.Get<bool>(2) && byColumns)
                {
                    _ = matching.Add(candidate.Get<uint>(0));
                }
            }
        }

        Hide([.. requests.Select(request => (request.Table, request.Lock))], matching, false);
    }

    /// <summary>
    /// Hides indexes on this node alone that a statement would use from it, as not live or as not valid. The statement
    /// takes the locks it takes on their tables first, as it would on its own, and then the share lock on them that
    /// PostgreSQL takes on an index it attaches, which no concurrent reindex or drop takes beside it, so that none
    /// changes their state meanwhile. Only those it would use are hidden, since the change holds them until the
    /// transaction ends, as attaching or using them would.
    /// </summary>
    /// <param name="tables">The tables, with the locks the statement takes on them.</param>
    /// <param name="matching">The indexes.</param>
    /// <param name="unlive">Whether they're hidden as not live, rather than not valid.</param>
    private static void Hide(List<(uint Table, PgLockMode Lock)> tables, HashSet<uint> matching, bool unlive)
    {
        if (matching.Count == 0)
        {
            return;
        }

        // The tables first, with the locks the statement takes on them: a foreign key's referenced table, a partitioned
        // index's partitions, or an attached partition and its partitions.
        foreach ((uint table, PgLockMode lockMode) in tables.Distinct().OrderBy(request => request.Table))
        {
            NativeMethods.LockRelationOid(table, (int)lockMode);
        }

        long[] relids = [.. matching.Order().Select(index => (long)index)];
        foreach (long index in relids)
        {
            NativeMethods.LockRelationOid((uint)index, (int)PgLockMode.Share);
        }

        WorfRows result = Elevated.Run(() => Worf.Query(unlive
            ? """
                UPDATE pg_catalog.pg_index i SET indislive = false FROM pg_catalog.pg_class t
                WHERE t.oid = i.indrelid AND i.indexrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[]) AND i.indislive
                RETURNING i.indexrelid, i.indrelid, t.relfilenode
                """
            : """
                UPDATE pg_catalog.pg_index i SET indisvalid = false
                WHERE i.indexrelid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[]) AND i.indisvalid AND i.indislive
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
