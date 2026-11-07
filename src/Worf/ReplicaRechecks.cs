using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Lets PostgreSQL recheck a table's deferrable unique and exclusion constraints for the rows worf writes under
/// <c>session_replication_role</c> replica, as it rechecks a session's rows: each constraint's recheck is an internal
/// trigger that fires on origin only, so PostgreSQL queues none for rows written as a replica, as for its own apply
/// worker's. While a writer of the table starts, which copies the table's trigger descriptor, the rechecks are marked to
/// fire always, and are marked back after. A relcache entry built again from the catalog meanwhile, as an invalidation
/// that another session's ANALYZE or ALTER TABLE sends does, is marked again as it's built, before the writer can copy
/// it. PostgreSQL then rechecks each row that its index found might conflict, at commit or, for a constraint that is
/// initially immediate, as the change ends, or at commit too for a writer that applies a transaction, whatever the
/// constraint's expressions and predicate.
/// </summary>
internal static unsafe partial class ReplicaRechecks
{
    // F_UNIQUE_KEY_RECHECK from utils/fmgroids.h, the function of every deferrable constraint's recheck trigger, and
    // TRIGGER_FIRES_ON_ORIGIN and TRIGGER_FIRES_ALWAYS from commands/trigger.h.
    private const uint UniqueKeyRecheck = 1250;
    private const sbyte FiresOnOrigin = (sbyte)'O';
    private const sbyte FiresAlways = (sbyte)'A';

    // The relcache entries marked now, which stay at their addresses as they're built again, with whether their
    // rechecks wait for commit and the triggers that were marked.
    private static readonly List<(nint Relation, bool UntilCommit, List<uint> Enabled, List<uint> Deferred)> s_marked = [];

    private static bool s_watching;

    /// <summary>
    /// Gets or sets whether a writer's rechecks of constraints that are initially immediate were made to wait for
    /// commit since this was last cleared.
    /// </summary>
    internal static bool Deferred { get; set; }

    [PgNativeCallback(nameof(RelcacheInvalidated))]
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static partial RelcacheCallbackFunction InvalidatedCallback { get; }
#else
    private static partial RelSyncCallbackFunction InvalidatedCallback { get; }
#endif

    /// <summary>
    /// Starts a writer of a table, as a row writer or a COPY, with the table's rechecks firing for its rows.
    /// </summary>
    /// <typeparam name="T">What starting the writer gives.</typeparam>
    /// <param name="reloid">The table.</param>
    /// <param name="start">Starts the writer.</param>
    /// <param name="untilCommit">Whether the rechecks of constraints that are initially immediate wait for commit too,
    /// as the writer applies a transaction whose statements' ends aren't known, at which PostgreSQL checks them, so that
    /// rows that swap values, which each statement left unique, aren't refused.</param>
    /// <returns>What starting the writer gave.</returns>
    internal static T Firing<T>(uint reloid, Func<T> start, bool untilCommit = false)
    {
        if (!s_watching)
        {
            NativeMethods.CacheRegisterRelcacheCallback(InvalidatedCallback, 0);
            s_watching = true;
        }

        // The table is locked first, as the writer would lock it, and its entry is held, so that the entry is built
        // again in place, where it's marked again, rather than dropped and built when the writer opens it.
        NativeMethods.LockRelationOid(reloid, (int)PgLockMode.RowExclusive);
        RelationData* relation = NativeMethods.RelationIdGetRelation(reloid);
        (nint Relation, bool UntilCommit, List<uint> Enabled, List<uint> Deferred) entry = ((nint)relation, untilCommit, [], []);
        Mark(entry);
        s_marked.Add(entry);
        try
        {
            return start();
        }
        finally
        {
            _ = s_marked.Remove(entry);
            TriggerDesc* trigdesc = relation->trigdesc;
            for (int i = 0; trigdesc is not null && i < trigdesc->numtriggers; i++)
            {
                Trigger* trigger = &trigdesc->triggers[i];
                if (entry.Enabled.Contains(trigger->tgoid))
                {
                    trigger->tgenabled = FiresOnOrigin;
                }

                if (entry.Deferred.Contains(trigger->tgoid))
                {
                    trigger->tginitdeferred = false;
                }
            }

            NativeMethods.RelationClose(relation);
        }
    }

    // Marks the rechecks of an entry's current descriptor, adding those it marks to those marked before. A descriptor
    // kept as the entry is built again, as when the catalog's matches it but for the marks, keeps its marks; the
    // catalog's triggers can't change meanwhile, as the table's lock keeps ALTER TABLE ... TRIGGER waiting.
    private static void Mark((nint Relation, bool UntilCommit, List<uint> Enabled, List<uint> Deferred) entry)
    {
        TriggerDesc* trigdesc = ((RelationData*)entry.Relation)->trigdesc;
        for (int i = 0; trigdesc is not null && i < trigdesc->numtriggers; i++)
        {
            Trigger* trigger = &trigdesc->triggers[i];
            if (!trigger->tgisinternal || trigger->tgfoid != UniqueKeyRecheck)
            {
                continue;
            }

            if (trigger->tgenabled == FiresOnOrigin)
            {
                trigger->tgenabled = FiresAlways;
                entry.Enabled.Add(trigger->tgoid);
            }

            if (entry.UntilCommit && !trigger->tginitdeferred)
            {
                trigger->tginitdeferred = true;
                entry.Deferred.Add(trigger->tgoid);
                Deferred = true;
            }
        }
    }

    // A held entry is built again in place as it's invalidated within a transaction, and its new descriptor is marked
    // here, before anything can copy it.
    private static void RelcacheInvalidated(ulong arg, uint reloid)
    {
        _ = arg;
        foreach ((nint Relation, bool UntilCommit, List<uint> Enabled, List<uint> Deferred) entry in s_marked)
        {
            RelationData* relation = (RelationData*)entry.Relation;
            if ((reloid == 0 || relation->rd_id == reloid) && relation->rd_isvalid)
            {
                Mark(entry);
            }
        }
    }
}
