using Ankus.Postgres;

[assembly: PgSql("queue-table", """
    CREATE TABLE worf.queue (
        queued_at timestamp with time zone NOT NULL,
        role name NOT NULL,
        replication_sets text[],
        message_type "char" NOT NULL,
        message json NOT NULL
    );
    """)]

namespace Worf;

/// <summary>
/// worf queue of replicated commands.
/// </summary>
internal static class Queue
{
    internal const char QueueCommandTypeSql = 'Q';
    internal const char QueueCommandTypeTruncate = 'T';
    internal const char QueueCommandTypeTablesync = 'A';
    internal const char QueueCommandTypeSequence = 'S';
    internal const char QueueCommandTypeDdl = 'D';

    private const string CatalogQueue = "queue";
    private const int NatsQueue = 5;
    private const int AnumQueueQueuedAt = 1;
    private const int AnumQueueRole = 2;
    private const int AnumQueueReplicationSets = 3;
    private const int AnumQueueMessageType = 4;
    private const int AnumQueueMessage = 5;

    // TRIGGER_TYPE_TRUNCATE and TRIGGER_TYPE_AFTER from commands/trigger.h.
    private const short TriggerTypeTruncate = 1 << 5;
    private const short TriggerTypeAfter = 0;

    private static uint s_queuetableoid;

    /// <summary>
    /// Add tuple to the queue table.
    /// </summary>
    /// <param name="replicationSets">The replication sets, or null.</param>
    /// <param name="roleoid">The role queueing the message.</param>
    /// <param name="messageType">The message type.</param>
    /// <param name="message">The message's JSON text.</param>
    internal static unsafe void QueueMessage(List<string>? replicationSets, uint roleoid, char messageType, string message)
    {
        long ts = Worker.GetCurrentTimestamp();
        sbyte* role = NativeMethods.GetUserNameFromId(roleoid, false);

        RelationData* rel = Worf.OpenCatalog(CatalogQueue, (int)PgLockMode.RowExclusive);
        TupleDescData* tupDesc = rel->rd_att;

        // Form a tuple.
        ulong* values = stackalloc ulong[NatsQueue];
        bool* nulls = stackalloc bool[NatsQueue];
        new Span<bool>(nulls, NatsQueue).Clear();

        values[AnumQueueQueuedAt - 1] = unchecked((ulong)ts);
        values[AnumQueueRole - 1] = (ulong)Worf.NameIn(role);
        if (replicationSets is not null)
        {
            values[AnumQueueReplicationSets - 1] = Worf.StrListToTextArray(replicationSets);
        }
        else
        {
            nulls[AnumQueueReplicationSets - 1] = true;
        }

        values[AnumQueueMessageType - 1] = (byte)messageType;
        // json_in stores the text it validates; worf builds its messages with escape_json.
        values[AnumQueueMessage - 1] = (ulong)NativeMethods.cstring_to_text(Worf.PStrDup(message));

        HeapTupleData* tup = NativeMethods.heap_form_tuple(tupDesc, values, nulls);

        // Insert the tuple to the catalog.
        NativeMethods.CatalogTupleInsert(rel, tup);

        // Cleanup.
        NativeMethods.heap_freetuple(tup);
        NativeMethods.table_close(rel, (int)PgLockMode.None);
    }

    /// <summary>
    /// Parse the tuple from the queue table into a <see cref="QueuedMessage"/>.
    /// </summary>
    /// <remarks>The caller must have the queue table locked in at least AccessShare mode.</remarks>
    /// <param name="queueTup">The queue tuple.</param>
    /// <returns>The message.</returns>
    internal static unsafe QueuedMessage QueuedMessageFromTuple(HeapTupleData* queueTup)
    {
        // Open relation to get the tuple descriptor.
        RelationData* rel = Worf.OpenCatalog(CatalogQueue, (int)PgLockMode.None);
        TupleDescData* tupDesc = rel->rd_att;
        QueuedMessage res = new();
        bool isnull;

        ulong d = NativeMethods.heap_getattr(queueTup, AnumQueueQueuedAt, tupDesc, &isnull);
        res.QueuedAt = unchecked((long)d);

        d = NativeMethods.heap_getattr(queueTup, AnumQueueRole, tupDesc, &isnull);
        res.Role = Worf.FromServer((sbyte*)d);

        d = NativeMethods.heap_getattr(queueTup, AnumQueueReplicationSets, tupDesc, &isnull);
        res.ReplicationSets = !isnull ? Worf.TextArrayDatumToList(d) : null;

        d = NativeMethods.heap_getattr(queueTup, AnumQueueMessageType, tupDesc, &isnull);
        res.MessageType = (char)(byte)d;

        d = NativeMethods.heap_getattr(queueTup, AnumQueueMessage, tupDesc, &isnull);
        // json keeps its text as given.
        res.Message = Worf.FromServer(NativeMethods.text_to_cstring((varlena*)d));

        // Close the relation.
        NativeMethods.table_close(rel, (int)PgLockMode.None);

        return res;
    }

    /// <summary>
    /// Get (cached) oid of the queue table.
    /// </summary>
    /// <returns>The queue table's oid.</returns>
    internal static uint GetQueueTableOid()
    {
        if (s_queuetableoid == 0)
        {
            s_queuetableoid = Worf.GetWorfTableOid(CatalogQueue);
        }

        return s_queuetableoid;
    }

    /// <summary>
    /// Create a TRUNCATE trigger for a persistent table and mark it tgisinternal so that it's not dumped by pg_dump.
    /// This is basically wrapper around CreateTrigger().
    /// </summary>
    /// <param name="rel">The table.</param>
    internal static unsafe void CreateTruncateTrigger(RelationData* rel)
    {
        uint* fargtypes = stackalloc uint[1];
        List* funcname = NativeMethods.lappend(null, NativeMethods.makeString(Worf.PStrDup(Worf.ExtensionName)));
        funcname = NativeMethods.lappend(funcname, NativeMethods.makeString(Worf.PStrDup("queue_truncate")));

        // Check for already existing trigger on the table to avoid adding duplicate ones.
        if (rel->trigdesc is not null)
        {
            Trigger* trigger = rel->trigdesc->triggers;
            uint funcoid = NativeMethods.LookupFuncName(funcname, 0, fargtypes, false);

            for (int i = 0; i < rel->trigdesc->numtriggers; i++, trigger++)
            {
                if ((trigger->tgtype & TriggerTypeTruncate) != 0 && trigger->tgfoid == funcoid)
                {
                    return;
                }
            }
        }

        CreateTrigStmt* tgstmt = (CreateTrigStmt*)NativeMethods.palloc0((ulong)sizeof(CreateTrigStmt));
        tgstmt->type = NodeTag.T_CreateTrigStmt;
        tgstmt->trigname = Worf.PStrDup("queue_truncate_trigger");
        tgstmt->relation = null;
        tgstmt->funcname = funcname;
        tgstmt->args = null;
        tgstmt->row = false;
        tgstmt->timing = TriggerTypeAfter;
        tgstmt->events = TriggerTypeTruncate;
        tgstmt->columns = null;
        tgstmt->whenClause = null;
        tgstmt->isconstraint = false;
        tgstmt->deferrable = false;
        tgstmt->initdeferred = false;
        tgstmt->constrrel = null;

        ObjectAddress trgobj = NativeMethods.CreateTrigger(tgstmt, null, rel->rd_id, 0, 0, 0, 0, 0, null,
            true /* tgisinternal */, false);

        ObjectAddress extension;
        extension.classId = Dependency.ExtensionRelationId;
        extension.objectId = NativeMethods.get_extension_oid(Worf.PStrDup(Worf.ExtensionName), false);
        extension.objectSubId = 0;

        NativeMethods.recordDependencyOn(&trgobj, &extension, DependencyType.DEPENDENCY_AUTO);

        // Make the new trigger visible within this session
        NativeMethods.CommandCounterIncrement();
    }
}
