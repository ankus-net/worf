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

    /// <summary>
    /// Checks, before a utility statement runs, whether it's DDL to queue for replication once it succeeds.
    /// </summary>
    /// <param name="pstmt">The statement.</param>
    /// <param name="context">Where it runs from.</param>
    /// <returns>Whether to queue it.</returns>
    internal static bool ShouldQueue(PlannedStmt* pstmt, ProcessUtilityContext context)
    {
        Node* parsetree = pstmt->utilityStmt;
        if (!EnableDdlReplication || Suppressed || NativeGlobals.creating_extension ||
            NativeGlobals.SessionReplicationRole == SessionReplicationRoleReplica ||
            (context != ProcessUtilityContext.PROCESS_UTILITY_TOPLEVEL && !AllowDdlFromFunctions) ||
            NativeMethods.GetCommandLogLevel(parsetree) != LogStmtLevel.LOGSTMT_DDL)
        {
            return false;
        }

        // Reorganizing a table's storage is the node's own.
#if ANKUS_PG19
        if (parsetree->type == NodeTag.T_RepackStmt)
#else
        if (parsetree->type == NodeTag.T_ClusterStmt)
#endif
        {
            return false;
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

            // Temporary objects are the session's, and worf's own the node's.
            NodeTag.T_CreateStmt => !StaysLocal(((CreateStmt*)parsetree)->relation),
            NodeTag.T_CreateTableAsStmt => !StaysLocal(((CreateTableAsStmt*)parsetree)->into->rel),
            NodeTag.T_ViewStmt => !StaysLocal(((ViewStmt*)parsetree)->view),
            NodeTag.T_CreateSeqStmt => !StaysLocal(((CreateSeqStmt*)parsetree)->sequence),
            NodeTag.T_AlterSeqStmt => !StaysLocal(((AlterSeqStmt*)parsetree)->sequence),
            NodeTag.T_IndexStmt => !StaysLocal(((IndexStmt*)parsetree)->relation),
            NodeTag.T_AlterTableStmt => !StaysLocal(((AlterTableStmt*)parsetree)->relation),
            NodeTag.T_DropStmt => !DropsLocal((DropStmt*)parsetree),

            // Each node creates, updates and drops worf itself.
            NodeTag.T_CreateExtensionStmt => !IsWorf(((CreateExtensionStmt*)parsetree)->extname),
            NodeTag.T_AlterExtensionStmt => !IsWorf(((AlterExtensionStmt*)parsetree)->extname),
            NodeTag.T_AlterExtensionContentsStmt => !IsWorf(((AlterExtensionContentsStmt*)parsetree)->extname),
            _ => true,
        };

        return replicated && WithSnapshot(() => WorfNode.GetLocalNode(false, true) is not null);
    }

    /// <summary>
    /// Queues a DDL statement that ran, with the search path it ran with.
    /// </summary>
    /// <param name="pstmt">The statement.</param>
    /// <param name="queryString">The text it came from.</param>
    internal static void QueueDdl(PlannedStmt* pstmt, sbyte* queryString)
    {
        int length = Worf.StrLen(queryString);
        int start = Math.Clamp(pstmt->stmt_location, 0, length);
        int count = pstmt->stmt_len > 0 ? Math.Min(pstmt->stmt_len, length - start) : length - start;
        string command = Worf.FromServer(new ReadOnlySpan<byte>((byte*)queryString + start, count)).Trim();

        sbyte* searchPath;
        fixed (byte* name = Worf.ToServerCString("search_path"))
        {
            searchPath = NativeMethods.GetConfigOption((sbyte*)name, false, false);
        }

        StringBuilder json = new("{\"search_path\": ");
        Executor.EscapeJson(json, searchPath is null ? "" : Worf.FromServer(searchPath));
        json.Append(", \"command\": ");
        Executor.EscapeJson(json, command);
        json.Append('}');

        _ = WithSnapshot(() =>
        {
            Queue.QueueMessage(["ddl_sql"], NativeMethods.GetUserId(), Queue.QueueCommandTypeDdl, json.ToString());
            return true;
        });
    }

    /// <summary>
    /// Adds a table that DDL created to the default replication set, or to the insert-only one when it has no
    /// replica identity, and moves a table from the insert-only set to the default one once it has.
    /// </summary>
    /// <param name="parsetree">The statement that ran.</param>
    internal static void AddCreatedTable(Node* parsetree)
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
        if (!IncludeDdlRepset || relation is null || NativeGlobals.creating_extension)
        {
            return;
        }

        uint reloid = NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        if (reloid != 0 && NativeMethods.get_rel_relkind(reloid) == (sbyte)'r' &&
            NativeMethods.get_rel_persistence(reloid) == (sbyte)'p' && NativeMethods.get_rel_namespace(reloid) != WorfNamespace())
        {
            _ = WithSnapshot(() => AddTable(parsetree, reloid));
        }
    }

    private static bool AddTable(Node* parsetree, uint reloid)
    {
        if (WorfNode.GetLocalNode(false, true) is not WorfLocalNode node)
        {
            return false;
        }

        RelationData* rel = NativeMethods.table_open(reloid, (int)PgLockMode.AccessShare);
        bool hasReplicaIndex = NativeMethods.RelationGetReplicaIndex(rel) != 0;
        NativeMethods.table_close(rel, (int)PgLockMode.None);

        List<string> sets = [.. ReplicationSet.GetTableReplicationSets(node.Node.Id, reloid).Select(set => set.Name)];
        WorfRepSet? defaultSet = ReplicationSet.GetReplicationSetByName(node.Node.Id, DefaultSet, true);
        WorfRepSet? insertOnlySet = ReplicationSet.GetReplicationSetByName(node.Node.Id, DefaultInsertOnlySet, true);
        if (parsetree->type != NodeTag.T_AlterTableStmt && sets.Count == 0)
        {
            if ((hasReplicaIndex ? defaultSet : insertOnlySet) is WorfRepSet set)
            {
                ReplicationSet.ReplicationSetAddTable(set.Id, reloid, [], null);
            }
        }
        else if (hasReplicaIndex && sets is [DefaultInsertOnlySet] && defaultSet is not null && insertOnlySet is not null)
        {
            ReplicationSet.ReplicationSetRemoveTable(insertOnlySet.Id, reloid, false);
            ReplicationSet.ReplicationSetAddTable(defaultSet.Id, reloid, [], null);
        }

        return true;
    }

    /// <summary>
    /// Runs work that reads worf's tables with the active snapshot, or with the transaction's when none is active,
    /// as after CREATE INDEX CONCURRENTLY. Taking one is left to statements that need it, since SET TRANSACTION
    /// can't follow it.
    /// </summary>
    private static bool WithSnapshot(Func<bool> work)
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
    /// Checks whether a DROP drops temporary relations, worf's own, worf itself, or PostgreSQL's own publications.
    /// </summary>
    private static bool DropsLocal(DropStmt* stmt)
    {
        if (stmt->removeType == ObjectType.OBJECT_PUBLICATION)
        {
            return true;
        }

        // An extension's or schema's name is a list of one name each, which NameListToString joins with dots.
        List* objects = stmt->objects;
        if (stmt->removeType is ObjectType.OBJECT_EXTENSION or ObjectType.OBJECT_SCHEMA)
        {
            return objects is not null &&
                Worf.FromServer(NativeMethods.NameListToString(objects)).Split('.').Contains(Worf.ExtensionName);
        }

        if (stmt->removeType is not (ObjectType.OBJECT_TABLE or ObjectType.OBJECT_VIEW or ObjectType.OBJECT_SEQUENCE or
            ObjectType.OBJECT_INDEX or ObjectType.OBJECT_MATVIEW or ObjectType.OBJECT_FOREIGN_TABLE))
        {
            return false;
        }

        for (int i = 0; objects is not null && i < objects->length; i++)
        {
            if (StaysLocal(NativeMethods.makeRangeVarFromNameList((List*)objects->elements[i].ptr_value)))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsWorf(sbyte* name) => name is not null && Worf.FromServer(name) == Worf.ExtensionName;

    private static uint WorfNamespace()
    {
        fixed (byte* name = Worf.ToServerCString(Worf.ExtensionName))
        {
            return NativeMethods.get_namespace_oid((sbyte*)name, true);
        }
    }
}
