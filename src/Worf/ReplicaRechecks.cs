using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Lets PostgreSQL recheck a table's deferrable unique and exclusion constraints for the rows worf writes under
/// <c>session_replication_role</c> replica, as it rechecks a session's rows: each constraint's recheck is an internal
/// trigger that fires on origin only, so PostgreSQL queues none for rows written as a replica, as for its own apply
/// worker's. While a writer of the table starts, which copies the table's trigger descriptor, the rechecks are marked to
/// fire always, and are marked back after. PostgreSQL then rechecks each row that its index found might conflict, at
/// commit or, for a constraint that is initially immediate, as the change ends, waiting for a transaction that writes a
/// conflicting row, whatever the constraint's expressions and predicate.
/// </summary>
internal static unsafe class ReplicaRechecks
{
    // F_UNIQUE_KEY_RECHECK from utils/fmgroids.h, the function of every deferrable constraint's recheck trigger, and
    // TRIGGER_FIRES_ON_ORIGIN and TRIGGER_FIRES_ALWAYS from commands/trigger.h.
    private const uint UniqueKeyRecheck = 1250;
    private const sbyte FiresOnOrigin = (sbyte)'O';
    private const sbyte FiresAlways = (sbyte)'A';

    /// <summary>
    /// Starts a writer of a table, as a row writer or a COPY, with the table's rechecks firing for its rows.
    /// </summary>
    /// <typeparam name="T">What starting the writer gives.</typeparam>
    /// <param name="reloid">The table.</param>
    /// <param name="start">Starts the writer.</param>
    /// <returns>What starting the writer gave.</returns>
    internal static T Firing<T>(uint reloid, Func<T> start)
    {
        // The table is locked first, as the writer would lock it, so that its relcache entry isn't built again from the
        // catalog between the marking and the writer's copy of the descriptor.
        NativeMethods.LockRelationOid(reloid, (int)PgLockMode.RowExclusive);
        RelationData* relation = NativeMethods.RelationIdGetRelation(reloid);
        TriggerDesc* trigdesc = relation->trigdesc;
        List<int> marked = [];
        try
        {
            for (int i = 0; trigdesc is not null && i < trigdesc->numtriggers; i++)
            {
                Trigger* trigger = &trigdesc->triggers[i];
                if (trigger->tgisinternal && trigger->tgfoid == UniqueKeyRecheck && trigger->tgenabled == FiresOnOrigin)
                {
                    trigger->tgenabled = FiresAlways;
                    marked.Add(i);
                }
            }

            return start();
        }
        finally
        {
            if (relation->trigdesc == trigdesc)
            {
                foreach (int i in marked)
                {
                    trigdesc->triggers[i].tgenabled = FiresOnOrigin;
                }
            }

            NativeMethods.RelationClose(relation);
        }
    }
}
