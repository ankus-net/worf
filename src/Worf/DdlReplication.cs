using System.Globalization;
using System.Text;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Replicates DDL as it runs, and adds the tables it creates to replication sets.
/// </summary>
internal static unsafe partial class DdlReplication
{
    // SESSION_REPLICATION_ROLE_REPLICA from commands/trigger.h.
    private const int SessionReplicationRoleReplica = 1;

    // RVR_MISSING_OK from catalog/namespace.h.
    private const uint RangeVarMissingOk = 1;

    private const string DefaultSet = "default";
    private const string DefaultInsertOnlySet = "default_insert_only";

    /// <summary>
    /// Gets whether DDL is replicated as it runs.
    /// </summary>
    [PgGucBool("worf.enable_ddl_replication", false, "Replicate DDL as it runs.", Context = PgGucContext.SuperuserSet)]
    internal static partial bool EnableDdlReplication { get; }

    /// <summary>
    /// Gets whether the tables DDL creates are added to the default replication sets.
    /// </summary>
    [PgGucBool("worf.include_ddl_repset", false, "Add the tables DDL creates to the default replication sets.",
        Context = PgGucContext.SuperuserSet)]
    internal static partial bool IncludeDdlRepset { get; }

    /// <summary>
    /// Gets whether DDL that functions and procedures run is replicated too.
    /// </summary>
    [PgGucBool("worf.allow_ddl_from_functions", false, "Replicate DDL that functions and procedures run.",
        Context = PgGucContext.SuperuserSet)]
    internal static partial bool AllowDdlFromFunctions { get; }

    /// <summary>
    /// Gets or sets whether DDL runs for <c>replicate_ddl_command</c>, which queues it itself.
    /// </summary>
    internal static bool Suppressed { get; set; }

    // Settings that change what a statement means, which travel with it.
    private static readonly string[] s_travelingSettings =
    [
        "search_path", "DateStyle", "IntervalStyle", "TimeZone", "standard_conforming_strings", "default_tablespace",
        "default_table_access_method", "default_toast_compression",
    ];

    // XACT_FLAGS_ACCESSEDTEMPNAMESPACE from access/xact.h.
    private const uint AccessedTempNamespace = 1;

    // How deep the session is in DDL being replicated: DDL that runs inside it, as from a function it calls, runs
    // again when it applies on the other nodes.
    private static int s_depth;

    // The objects that the DDL being replicated creates or changes, and whether it runs or inlines one of the
    // session's temporary functions, noted while the session has temporary objects. PostgreSQL notes only the use of
    // temporary tables, so an object that depends on another temporary object is found by its dependencies.
    private static readonly List<(uint ClassId, uint ObjectId, int SubId)> s_changedObjects = [];
    private static bool s_ranTemporaryFunction;

    /// <summary>
    /// Decides, before a utility statement runs, whether it's DDL to queue for replication once it succeeds, and the
    /// command to queue: the statement itself, or for a DROP of local and other objects, a DROP of the others.
    /// </summary>
    /// <param name="pstmt">The statement.</param>
    /// <param name="queryString">The text it came from.</param>
    /// <param name="context">Where it runs from.</param>
    /// <param name="local">Whether it uses the session's own objects, so that what it creates stays on this node.</param>
    /// <returns>The command to queue, or null.</returns>
    internal static string? CommandToQueue(PlannedStmt* pstmt, sbyte* queryString, ProcessUtilityContext context, out bool local)
    {
        Node* parsetree = pstmt->utilityStmt;
        local = parsetree->type == NodeTag.T_CreateTableAsStmt && ExecutesPrepared((CreateTableAsStmt*)parsetree);

        // Statements PostgreSQL runs as parts of another, and DDL inside DDL being replicated, replicate with it.
        if (!EnableDdlReplication || Suppressed || NativeGlobals.creating_extension || s_depth > 0 ||
            NativeGlobals.SessionReplicationRole == SessionReplicationRoleReplica ||
            context == ProcessUtilityContext.PROCESS_UTILITY_SUBCOMMAND ||
            (context != ProcessUtilityContext.PROCESS_UTILITY_TOPLEVEL && !AllowDdlFromFunctions) ||
            NativeMethods.GetCommandLogLevel(parsetree) != LogStmtLevel.LOGSTMT_DDL || Restoring())
        {
            return null;
        }

        // Reorganizing a table's storage is the node's own.
#if ANKUS_PG19
        if (parsetree->type == NodeTag.T_RepackStmt)
#else
        if (parsetree->type == NodeTag.T_ClusterStmt)
#endif
        {
            return null;
        }

        bool replicated = parsetree->type switch
        {
            // Databases, tablespaces, roles, settings and PostgreSQL's own replication are the cluster's, not the
            // database's.
            NodeTag.T_CreatedbStmt or NodeTag.T_DropdbStmt or NodeTag.T_AlterDatabaseStmt or
                NodeTag.T_AlterDatabaseSetStmt or NodeTag.T_AlterDatabaseRefreshCollStmt or
                NodeTag.T_CreateTableSpaceStmt or NodeTag.T_DropTableSpaceStmt or NodeTag.T_AlterTableSpaceOptionsStmt or
                NodeTag.T_CreateRoleStmt or NodeTag.T_AlterRoleStmt or NodeTag.T_AlterRoleSetStmt or
                NodeTag.T_DropRoleStmt or NodeTag.T_GrantRoleStmt or NodeTag.T_AlterSystemStmt or
                NodeTag.T_CreateSubscriptionStmt or NodeTag.T_AlterSubscriptionStmt or NodeTag.T_DropSubscriptionStmt or
                NodeTag.T_CreatePublicationStmt or NodeTag.T_AlterPublicationStmt => false,
            NodeTag.T_RenameStmt when IsClusterObject(((RenameStmt*)parsetree)->renameType) => false,
            NodeTag.T_AlterOwnerStmt when IsClusterObject(((AlterOwnerStmt*)parsetree)->objectType) => false,
            NodeTag.T_CommentStmt when IsClusterObject(((CommentStmt*)parsetree)->objtype) => false,
            NodeTag.T_SecLabelStmt when IsClusterObject(((SecLabelStmt*)parsetree)->objtype) => false,
            NodeTag.T_GrantStmt when IsClusterObject(((GrantStmt*)parsetree)->objtype) => false,

            // A prepared statement is the session's.
            NodeTag.T_CreateTableAsStmt when local => false,

            // Each node creates, updates and drops worf itself.
            NodeTag.T_CreateExtensionStmt => !IsWorf(((CreateExtensionStmt*)parsetree)->extname),
            NodeTag.T_AlterExtensionStmt => !IsWorf(((AlterExtensionStmt*)parsetree)->extname),
            NodeTag.T_AlterExtensionContentsStmt => !IsWorf(((AlterExtensionContentsStmt*)parsetree)->extname),
            NodeTag.T_DropStmt => true,

            // Temporary objects are the session's, and worf's own the node's. Finishing a concurrent detach is the
            // node's own too, since the others detached the partition at once.
            NodeTag.T_AlterTableStmt when FinishesDetach((AlterTableStmt*)parsetree) => false,
            _ => !TargetsLocal(parsetree),
        };

        if (!replicated || !WithSnapshot(() => WorfNode.GetLocalNode(false, true) is not null))
        {
            return null;
        }

        int length = Worf.StrLen(queryString);
        int start = Math.Clamp(pstmt->stmt_location, 0, length);
        int count = pstmt->stmt_len > 0 ? Math.Min(pstmt->stmt_len, length - start) : length - start;
        string command = Worf.FromServer(new ReadOnlySpan<byte>((byte*)queryString + start, count)).Trim();
        return parsetree->type == NodeTag.T_DropStmt ? DropCommand((DropStmt*)parsetree, command) : command;
    }

    /// <summary>
    /// Starts running a utility statement, noting from here on whether it uses temporary objects.
    /// </summary>
    /// <param name="replicated">Whether it's DDL being replicated.</param>
    /// <returns>The transaction's flags before it, for <see cref="EndStatement"/>.</returns>
    internal static uint BeginStatement(bool replicated)
    {
        uint flags = (uint)NativeGlobals.MyXactFlags;
        NativeGlobals.MyXactFlags = (int)(flags & ~AccessedTempNamespace);
        if (replicated && s_depth++ == 0)
        {
            s_changedObjects.Clear();
            s_ranTemporaryFunction = false;
        }

        return flags;
    }

    /// <summary>
    /// Gets whether DDL being replicated runs now, as when a statement that commits partway, such as
    /// <c>CREATE INDEX CONCURRENTLY</c>, commits.
    /// </summary>
    internal static bool Running => s_depth > 0;

    /// <summary>
    /// Notes an object that DDL being replicated creates or changes, from the object access hook, to find whether it
    /// uses the session's temporary objects once it has run.
    /// </summary>
    /// <param name="access">What the DDL does with the object.</param>
    /// <param name="classId">The object's catalog.</param>
    /// <param name="objectId">The object.</param>
    /// <param name="subId">The object's column, or zero.</param>
    internal static void NoteObjectAccess(ObjectAccessType access, uint classId, uint objectId, int subId)
    {
        if (access is ObjectAccessType.OAT_POST_CREATE or ObjectAccessType.OAT_POST_ALTER && s_depth > 0 && TemporaryNamespace() != 0)
        {
            s_changedObjects.Add((classId, objectId, subId));
        }
    }

    /// <summary>
    /// Notes a function that DDL being replicated is about to run or inline, as an expression of
    /// <c>ALTER TABLE ... USING</c> does, which leaves no dependency behind.
    /// </summary>
    /// <param name="funcid">The function.</param>
    internal static void NoteFunctionUse(uint funcid)
    {
        if (s_depth > 0 && TemporaryNamespace() != 0 && NativeMethods.isTempNamespace(NativeMethods.get_func_namespace(funcid)))
        {
            s_ranTemporaryFunction = true;
        }
    }

    /// <summary>
    /// Checks whether DDL being replicated that has run used the session's temporary objects other than tables,
    /// which PostgreSQL doesn't note: ran or inlined a temporary function, changed a temporary object, made an object
    /// depend on one, or commented on, labeled or granted on one. A DROP leaves out its temporary objects itself.
    /// </summary>
    /// <param name="parsetree">The DDL.</param>
    /// <returns>Whether it did.</returns>
    internal static bool UsedTemporaryObjects(Node* parsetree)
    {
        uint nspoid = TemporaryNamespace();
        if (nspoid == 0 || parsetree->type == NodeTag.T_DropStmt)
        {
            return false;
        }

        if (s_ranTemporaryFunction)
        {
            return true;
        }

        AddTargets(parsetree);
        if (s_changedObjects.Count == 0)
        {
            return false;
        }

        NativeMethods.CommandCounterIncrement();
        string schema = Worf.FromServer(NativeMethods.get_namespace_name(nspoid));
        long[] classIds = [.. s_changedObjects.Select(changed => (long)changed.ClassId)];
        long[] objectIds = [.. s_changedObjects.Select(changed => (long)changed.ObjectId)];
        long[] subIds = [.. s_changedObjects.Select(changed => (long)changed.SubId)];
        return Elevated.Run(() => Spi.ExecuteScalar<bool>("""
            SELECT EXISTS (
                SELECT FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.oid[]), pg_catalog.unnest($2::pg_catalog.oid[]),
                    pg_catalog.unnest($3::pg_catalog.int4[])) AS o (classid, objid, objsubid)
                WHERE (pg_catalog.pg_identify_object(o.classid, o.objid, 0)).schema = $4
                    OR EXISTS (
                        SELECT FROM pg_catalog.pg_depend d
                        WHERE d.classid = o.classid AND d.objid = o.objid AND (o.objsubid = 0 OR d.objsubid IN (0, o.objsubid))
                            AND (pg_catalog.pg_identify_object(d.refclassid, d.refobjid, 0)).schema = $4))
            """, SpiParameter.Create(classIds), SpiParameter.Create(objectIds), SpiParameter.Create(subIds), SpiParameter.Create(schema)));
    }

    /// <summary>
    /// Adds the objects that COMMENT, SECURITY LABEL and GRANT name to the changed objects, which the object access
    /// hook doesn't see.
    /// </summary>
    private static void AddTargets(Node* parsetree)
    {
        switch (parsetree->type)
        {
            case NodeTag.T_CommentStmt:
                AddTarget(((CommentStmt*)parsetree)->objtype, ((CommentStmt*)parsetree)->@object);
                break;

            case NodeTag.T_SecLabelStmt:
                AddTarget(((SecLabelStmt*)parsetree)->objtype, ((SecLabelStmt*)parsetree)->@object);
                break;

            case NodeTag.T_GrantStmt when ((GrantStmt*)parsetree)->targtype == GrantTargetType.ACL_TARGET_OBJECT:
                GrantStmt* grant = (GrantStmt*)parsetree;
                List* objects = grant->objects;
                for (int i = 0; objects is not null && i < objects->length; i++)
                {
                    Node* target = (Node*)objects->elements[i].ptr_value;
                    if (grant->objtype is ObjectType.OBJECT_FUNCTION or ObjectType.OBJECT_PROCEDURE or ObjectType.OBJECT_ROUTINE)
                    {
                        AddTarget(grant->objtype, target);
                    }
                    else if (grant->objtype is ObjectType.OBJECT_TYPE or ObjectType.OBJECT_DOMAIN)
                    {
                        AddTarget(grant->objtype, (Node*)NativeMethods.makeTypeNameFromNameList((List*)target));
                    }
                }

                break;

            default:
                break;
        }
    }

    private static void AddTarget(ObjectType type, Node* target)
    {
        RelationData* relation = null;
        ObjectAddress address = NativeMethods.get_object_address(type, target, &relation, (int)PgLockMode.AccessShare, true);
        if (relation is not null)
        {
            NativeMethods.relation_close(relation, (int)PgLockMode.None);
        }

        if (address.objectId != 0)
        {
            s_changedObjects.Add((address.classId, address.objectId, address.objectSubId));
        }
    }

    /// <summary>
    /// Checks whether an object a DROP names is one of the session's temporary objects, locking it as the DROP will.
    /// </summary>
    private static bool IsTemporary(ObjectType type, Node* target)
    {
        if (TemporaryNamespace() == 0)
        {
            return false;
        }

        RelationData* relation = null;
        ObjectAddress address = NativeMethods.get_object_address(type, target, &relation, (int)PgLockMode.AccessExclusive, true);
        if (relation is not null)
        {
            bool temporary = relation->rd_rel->relpersistence == (sbyte)'t';
            NativeMethods.relation_close(relation, (int)PgLockMode.None);
            return temporary;
        }

        uint nspoid = address.objectId == 0 ? 0 : NativeMethods.get_object_namespace(&address);
        return nspoid != 0 && NativeMethods.isTempNamespace(nspoid);
    }

    /// <summary>
    /// Gets the session's temporary namespace, or zero while it has none.
    /// </summary>
    private static uint TemporaryNamespace()
    {
        uint nspoid;
        uint toastoid;
        NativeMethods.GetTempNamespaceState(&nspoid, &toastoid);
        return nspoid;
    }

    /// <summary>
    /// Ends running a utility statement, which has run or failed.
    /// </summary>
    /// <param name="flags">The transaction's flags before it.</param>
    /// <param name="replicated">Whether it's DDL being replicated.</param>
    /// <returns>Whether it used temporary objects.</returns>
    internal static bool EndStatement(uint flags, bool replicated)
    {
        if (replicated)
        {
            s_depth--;
        }

        bool usedTemp = ((uint)NativeGlobals.MyXactFlags & AccessedTempNamespace) != 0;
        NativeGlobals.MyXactFlags |= (int)(flags & AccessedTempNamespace);
        return usedTemp;
    }

    /// <summary>
    /// Queues a DDL statement that ran, with the settings that change what it means.
    /// </summary>
    /// <param name="command">The statement.</param>
    internal static void QueueDdl(string command)
    {
        StringBuilder json = new("{\"search_path\": ");
        Executor.EscapeJson(json, Setting("search_path"));
        json.Append(", \"settings\": {");
        for (int i = 1; i < s_travelingSettings.Length; i++)
        {
            json.Append(i > 1 ? ", " : "");
            Executor.EscapeJson(json, s_travelingSettings[i]);
            json.Append(": ");
            Executor.EscapeJson(json, Setting(s_travelingSettings[i]));
        }

        json.Append("}, \"command\": ");
        Executor.EscapeJson(json, command);

        // DDL that locked the other nodes tells each node the fence to release and how far to apply first.
        if (DdlLock.TakeStatementFence() is (ulong fenceId, List<(uint NodeId, ulong Lsn)> positions))
        {
            json.Append(CultureInfo.InvariantCulture, $", \"fence\": \"{fenceId}\", \"positions\": {{");
            for (int i = 0; i < positions.Count; i++)
            {
                json.Append(CultureInfo.InvariantCulture, $"{(i == 0 ? "" : ", ")}\"{positions[i].NodeId}\": \"{Worf.FormatLsn(positions[i].Lsn)}\"");
            }

            json.Append('}');
        }

        json.Append('}');

        _ = WithSnapshot(() =>
        {
            Queue.QueueMessage(["ddl_sql"], NativeMethods.GetUserId(), Queue.QueueCommandTypeDdl, json.ToString());
            return true;
        });
    }

    /// <summary>
    /// Gets whether this session restores a subscription's structure. Only a superuser, or a role granted
    /// <c>SET</c> on it, may set it, since it turns off DDL replication, the global DDL lock and sequence range checks.
    /// </summary>
    [PgGucBool("worf.subscription_schema_restore", false, "Marks a session that restores a subscription's structure.",
        Context = PgGucContext.SuperuserSet)]
    internal static partial bool SubscriptionSchemaRestore { get; }

    /// <summary>
    /// Checks whether this session restores a subscription's structure, which DDL replication leaves alone, since the
    /// nodes it would reach already have it.
    /// </summary>
    /// <returns>Whether it does.</returns>
    internal static bool Restoring() => SubscriptionSchemaRestore;

    private static string Setting(string name)
    {
        fixed (byte* cname = Worf.ToServerCString(name))
        {
            sbyte* value = NativeMethods.GetConfigOption((sbyte*)cname, true, false);
            return value is null ? "" : Worf.FromServer(value);
        }
    }

    /// <summary>
    /// Keeps the tables DDL creates or changes in the replication sets that suit them. Under
    /// <c>worf.include_ddl_repset</c>, a new table joins the default set, or the insert-only one while it has no
    /// replica identity, and a table and its partitions move between the two as they gain or lose one. A table that
    /// becomes unlogged, whose changes can't replicate, leaves its sets, or with the setting off, warns.
    /// </summary>
    /// <param name="parsetree">The statement that ran.</param>
    internal static void AfterDdl(Node* parsetree)
    {
        RangeVar* relation = parsetree->type switch
        {
            NodeTag.T_CreateStmt => ((CreateStmt*)parsetree)->relation,
            NodeTag.T_CreateTableAsStmt when ((CreateTableAsStmt*)parsetree)->objtype == ObjectType.OBJECT_TABLE =>
                ((CreateTableAsStmt*)parsetree)->into->rel,
            NodeTag.T_AlterTableStmt => ((AlterTableStmt*)parsetree)->relation,
            _ => null,
        };

        // Extensions' tables, such as worf's own, aren't replicated.
        if (relation is null || NativeGlobals.creating_extension)
        {
            return;
        }

        uint reloid = NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        if (reloid == 0 || NativeMethods.get_rel_namespace(reloid) == WorfNamespace())
        {
            return;
        }

        // The replication sets, and the truncate trigger a table joining one gets, are worf's own, whoever runs the DDL.
        bool join = parsetree->type != NodeTag.T_AlterTableStmt || HasCommand((AlterTableStmt*)parsetree, AlterTableType.AT_SetLogged);
        _ = WithSnapshot(() => Elevated.Run(() => Reconcile(reloid, join)));
    }

    private static bool Reconcile(uint reloid, bool join)
    {
        if (WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
        {
            return false;
        }

        WorfRepSet? defaultSet = ReplicationSet.GetReplicationSetByName(node.Node.Id, DefaultSet, true);
        WorfRepSet? insertOnlySet = ReplicationSet.GetReplicationSetByName(node.Node.Id, DefaultInsertOnlySet, true);
        List* relids = NativeMethods.find_all_inheritors(reloid, 0, null);
        for (int i = 0; relids is not null && i < relids->length; i++)
        {
            uint relid = relids->elements[i].oid_value;
            if (NativeMethods.get_rel_relkind(relid) != (sbyte)'r')
            {
                continue;
            }

            string name = Worf.FromServer(NativeMethods.get_rel_name(relid));
            List<WorfRepSet> sets = ReplicationSet.GetTableReplicationSets(node.Node.Id, relid);
            sbyte persistence = NativeMethods.get_rel_persistence(relid);
            if (persistence == (sbyte)'u')
            {
                if (sets.Count > 0 && IncludeDdlRepset)
                {
                    sets.ForEach(set => ReplicationSet.ReplicationSetRemoveTable(set.Id, relid, false));
                    PgLog.Notice($"table {name} is unlogged now, so it left its replication sets");
                }
                else if (sets.Count > 0)
                {
                    PgLog.Warning($"table {name} is unlogged now, so its changes no longer replicate");
                }

                continue;
            }

            if (!IncludeDdlRepset || persistence != (sbyte)'p' || defaultSet is null || insertOnlySet is null)
            {
                continue;
            }

            RelationData* rel = NativeMethods.table_open(relid, (int)PgLockMode.AccessShare);
            bool hasReplicaIndex = NativeMethods.RelationGetReplicaIndex(rel) != 0;
            NativeMethods.table_close(rel, (int)PgLockMode.None);

            bool inDefault = sets.Exists(set => set.Id == defaultSet.Id);
            bool inInsertOnly = sets.Exists(set => set.Id == insertOnlySet.Id);
            if (sets.Count == 0 && join && relid == reloid)
            {
                ReplicationSet.ReplicationSetAddTable(hasReplicaIndex ? defaultSet.Id : insertOnlySet.Id, relid, [], null);
            }
            else if (hasReplicaIndex && inInsertOnly && sets.Count == 1)
            {
                ReplicationSet.ReplicationSetRemoveTable(insertOnlySet.Id, relid, false);
                ReplicationSet.ReplicationSetAddTable(defaultSet.Id, relid, [], null);
            }
            else if (!hasReplicaIndex && inDefault)
            {
                // Updates and deletes of a table with no replica identity can't apply elsewhere.
                ReplicationSet.ReplicationSetRemoveTable(defaultSet.Id, relid, false);
                if (!inInsertOnly)
                {
                    ReplicationSet.ReplicationSetAddTable(insertOnlySet.Id, relid, [], null);
                }
            }
        }

        return true;
    }

    private static bool HasCommand(AlterTableStmt* stmt, AlterTableType subtype)
    {
        List* cmds = stmt->cmds;
        for (int i = 0; cmds is not null && i < cmds->length; i++)
        {
            if (((AlterTableCmd*)cmds->elements[i].ptr_value)->subtype == subtype)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Runs work that reads worf's tables with the active snapshot, or with the transaction's when none is active,
    /// as after CREATE INDEX CONCURRENTLY. Taking one is left to statements that need it, since SET TRANSACTION
    /// can't follow it.
    /// </summary>
    internal static bool WithSnapshot(Func<bool> work)
    {
        if (NativeMethods.ActiveSnapshotSet())
        {
            return work();
        }

        NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());
        try
        {
            return work();
        }
        finally
        {
            NativeMethods.PopActiveSnapshot();
        }
    }

    /// <summary>
    /// Checks whether a relation is temporary, or one of worf's own.
    /// </summary>
    private static bool StaysLocal(RangeVar* relation)
    {
        if (relation is null)
        {
            return false;
        }

        if (relation->relpersistence == (sbyte)'t')
        {
            return true;
        }

        if (relation->schemaname is not null)
        {
            string schema = Worf.FromServer(relation->schemaname);
            if (schema == Worf.ExtensionName || schema.StartsWith("pg_temp", StringComparison.Ordinal))
            {
                return true;
            }
        }

        uint reloid = NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        return reloid != 0 &&
            (NativeMethods.get_rel_persistence(reloid) == (sbyte)'t' || NativeMethods.get_rel_namespace(reloid) == WorfNamespace());
    }

    /// <summary>
    /// Checks whether a statement's target is a temporary relation or one of worf's own, or worf's schema. Statements
    /// that use temporary objects otherwise are found as they run.
    /// </summary>
    private static bool TargetsLocal(Node* parsetree)
    {
        switch (parsetree->type)
        {
            case NodeTag.T_CreateStmt:
                return StaysLocal(((CreateStmt*)parsetree)->relation);
            case NodeTag.T_CreateTableAsStmt:
                return StaysLocal(((CreateTableAsStmt*)parsetree)->into->rel);
            case NodeTag.T_ViewStmt:
                return StaysLocal(((ViewStmt*)parsetree)->view);
            case NodeTag.T_CreateSeqStmt:
                return StaysLocal(((CreateSeqStmt*)parsetree)->sequence);
            case NodeTag.T_AlterSeqStmt:
                return StaysLocal(((AlterSeqStmt*)parsetree)->sequence);
            case NodeTag.T_IndexStmt:
                return StaysLocal(((IndexStmt*)parsetree)->relation);
            case NodeTag.T_AlterTableStmt:
                return StaysLocal(((AlterTableStmt*)parsetree)->relation);
            case NodeTag.T_CreateTrigStmt:
                return StaysLocal(((CreateTrigStmt*)parsetree)->relation);
            case NodeTag.T_CreatePolicyStmt:
                return StaysLocal(((CreatePolicyStmt*)parsetree)->table);
            case NodeTag.T_AlterPolicyStmt:
                return StaysLocal(((AlterPolicyStmt*)parsetree)->table);
            case NodeTag.T_RuleStmt:
                return StaysLocal(((RuleStmt*)parsetree)->relation);
            case NodeTag.T_RenameStmt:
                return StaysLocal(((RenameStmt*)parsetree)->relation);
            case NodeTag.T_AlterObjectSchemaStmt:
                return StaysLocal(((AlterObjectSchemaStmt*)parsetree)->relation);
            case NodeTag.T_AlterOwnerStmt:
                return StaysLocal(((AlterOwnerStmt*)parsetree)->relation);
            case NodeTag.T_GrantStmt:
                GrantStmt* grant = (GrantStmt*)parsetree;
                List* objects = grant->objects;
                for (int i = 0; objects is not null && i < objects->length; i++)
                {
                    void* target = objects->elements[i].ptr_value;
                    bool local = grant->targtype == GrantTargetType.ACL_TARGET_ALL_IN_SCHEMA
                        ? NameIs((Node*)target, Worf.ExtensionName)
                        : grant->objtype is ObjectType.OBJECT_TABLE or ObjectType.OBJECT_SEQUENCE && StaysLocal((RangeVar*)target);
                    if (local)
                    {
                        return true;
                    }
                }

                return false;
            default:
                return false;
        }
    }

    /// <summary>
    /// Returns the DROP to queue: none when every object stays local, the statement itself when none does, and a DROP
    /// of the others otherwise. worf, which each node drops itself, can't be dropped with other objects.
    /// </summary>
    private static string? DropCommand(DropStmt* stmt, string command)
    {
        List* objects = stmt->objects;
        int count = objects is null ? 0 : objects->length;
        switch (stmt->removeType)
        {
            case ObjectType.OBJECT_PUBLICATION:
                return null;

            case ObjectType.OBJECT_EXTENSION or ObjectType.OBJECT_SCHEMA:
                int worf = 0;
                for (int i = 0; i < count; i++)
                {
                    worf += NameIs((Node*)objects->elements[i].ptr_value, Worf.ExtensionName) ? 1 : 0;
                }

                return worf == 0 ? command
                    : worf == count ? null
                    : throw new PgException("55000", "worf can't be dropped together with other objects while worf.enable_ddl_replication is on",
                        hint: "Drop it in a statement of its own.");

            // A trigger's, policy's or rule's name follows its table's.
            case ObjectType.OBJECT_TRIGGER or ObjectType.OBJECT_POLICY or ObjectType.OBJECT_RULE:
                for (int i = 0; i < count; i++)
                {
                    List* names = (List*)objects->elements[i].ptr_value;
                    if (names is not null && names->length > 1 &&
                        StaysLocal(NativeMethods.makeRangeVarFromNameList(NativeMethods.list_truncate(NativeMethods.list_copy(names), names->length - 1))))
                    {
                        return null;
                    }
                }

                return command;

            case ObjectType.OBJECT_TABLE or ObjectType.OBJECT_VIEW or ObjectType.OBJECT_MATVIEW or ObjectType.OBJECT_SEQUENCE or
                ObjectType.OBJECT_INDEX or ObjectType.OBJECT_FOREIGN_TABLE:
                List<string> kept = [];
                for (int i = 0; i < count; i++)
                {
                    List* names = (List*)objects->elements[i].ptr_value;
                    if (!StaysLocal(NativeMethods.makeRangeVarFromNameList(names)))
                    {
                        kept.Add(Worf.FromServer(NativeMethods.NameListToQuotedString(names)));
                    }
                }

                if (kept.Count == count)
                {
                    return command;
                }

                if (kept.Count == 0)
                {
                    return null;
                }

                string kind = stmt->removeType switch
                {
                    ObjectType.OBJECT_TABLE => "TABLE",
                    ObjectType.OBJECT_VIEW => "VIEW",
                    ObjectType.OBJECT_MATVIEW => "MATERIALIZED VIEW",
                    ObjectType.OBJECT_SEQUENCE => "SEQUENCE",
                    ObjectType.OBJECT_INDEX => "INDEX",
                    _ => "FOREIGN TABLE",
                };
                return $"DROP {kind}{(stmt->concurrent ? " CONCURRENTLY" : "")}{(stmt->missing_ok ? " IF EXISTS" : "")} " +
                    $"{string.Join(", ", kept)}{(stmt->behavior == DropBehavior.DROP_CASCADE ? " CASCADE" : "")}";

            // Any other object stays on this node when it's temporary.
            default:
                int temporary = 0;
                for (int i = 0; i < count; i++)
                {
                    temporary += IsTemporary(stmt->removeType, (Node*)objects->elements[i].ptr_value) ? 1 : 0;
                }

                return temporary == 0 ? command
                    : temporary == count ? null
                    : throw new PgException("55000", "temporary objects can't be dropped together with other objects while worf.enable_ddl_replication is on",
                        hint: "Drop them in a statement of their own.");
        }
    }

    /// <summary>
    /// Checks whether CREATE TABLE AS runs a prepared statement, which parse analysis wraps in a utility query.
    /// </summary>
    private static bool ExecutesPrepared(CreateTableAsStmt* stmt)
    {
        Node* query = stmt->query;
        return query is not null && (query->type == NodeTag.T_ExecuteStmt ||
            (query->type == NodeTag.T_Query && ((Query*)query)->commandType == CmdType.CMD_UTILITY &&
                ((Query*)query)->utilityStmt is not null && ((Query*)query)->utilityStmt->type == NodeTag.T_ExecuteStmt));
    }

    /// <summary>
    /// Checks whether ALTER TABLE finishes a concurrent detach.
    /// </summary>
    private static bool FinishesDetach(AlterTableStmt* stmt) => HasCommand(stmt, AlterTableType.AT_DetachPartitionFinalize);

    /// <summary>
    /// Checks whether a name in a statement's list of names, a String node, is a given name.
    /// </summary>
    private static bool NameIs(Node* name, string expected)
        => name is not null && Worf.FromServer(NativeMethods.NameListToString(NativeMethods.lappend(null, name))) == expected;

    /// <summary>
    /// Checks whether objects of a type are the cluster's, or PostgreSQL's own replication's, rather than the
    /// database's.
    /// </summary>
    private static bool IsClusterObject(ObjectType type)
        => type is ObjectType.OBJECT_DATABASE or ObjectType.OBJECT_TABLESPACE or ObjectType.OBJECT_ROLE or
            ObjectType.OBJECT_PARAMETER_ACL or ObjectType.OBJECT_PUBLICATION or ObjectType.OBJECT_SUBSCRIPTION;

    private static bool IsWorf(sbyte* name) => name is not null && Worf.FromServer(name) == Worf.ExtensionName;

    private static uint WorfNamespace()
    {
        fixed (byte* name = Worf.ToServerCString(Worf.ExtensionName))
        {
            return NativeMethods.get_namespace_oid((sbyte*)name, true);
        }
    }
}
