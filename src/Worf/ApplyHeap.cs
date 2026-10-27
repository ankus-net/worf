using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf apply functions using heap api.
/// </summary>
internal static unsafe class ApplyHeap
{
    // A writer keeps its executor state until it closes, so a transaction that writes to many tables keeps only the
    // latest few open, as pglogical keeps one.
    private const int MaxWriters = 64;

    // The writers of the remote transaction being applied, by table, with a row for its changes, one for their keys
    // and the table's replica identity index.
    private static readonly Dictionary<uint, (PgRelationWriter Writer, PgReplicatedRow Row, PgReplicatedRow Key, uint ReplicaIndex)> s_writers = [];

    // The tables of those writers, in the order they were opened.
    private static readonly Queue<uint> s_writerOrder = [];

    /// <summary>
    /// Handle the start of a remote transaction.
    /// </summary>
    internal static void ApplyHeapBegin()
    {
        // A transaction that failed has released what its writers held.
        s_writers.Clear();
        s_writerOrder.Clear();
    }

    /// <summary>
    /// Handle the end of a remote transaction.
    /// </summary>
    internal static void ApplyHeapCommit()
    {
        // A writer has to be closed before its transaction commits.
        foreach ((PgRelationWriter writer, _, _, _) in s_writers.Values)
        {
            writer.Dispose();
        }

        s_writers.Clear();
        s_writerOrder.Clear();
    }

    /// <summary>
    /// Handle insert via low level api.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="newtup">The new tuple.</param>
    internal static void ApplyHeapInsert(WorfRelation rel, WorfTupleData newtup)
    {
        (PgRelationWriter writer, PgReplicatedRow row, _, _) = Writer(rel);
        PgHeapTuple? heaprow = SetRow(row, newtup) ? null : HeapRow(rel, newtup);

        // Check for existing tuple with same key in any unique index containing only normal columns. This doesn't
        // just check the replica identity index, but it'll prefer it and use it first.
        PgFoundRow? local = heaprow is null ? writer.FindConflict(row) : writer.FindConflict(heaprow);

        if (local is null)
        {
            if (heaprow is null)
            {
                writer.Insert(row);
            }
            else
            {
                writer.Insert(heaprow);
            }

            return;
        }

        // Tuple already exists, try resolving conflict.
        RelationData* relation = rel.Rel;
        HeapTupleData* localtuple = LocalTuple(local);
        HeapTupleData* remotetuple = FormTuple(relation, newtup);
        long localTs = local.CommitTime?.MicrosecondsSinceEpoch ?? 0;

        bool apply = Conflict.TryResolveConflict(local.CommitOrigin, localTs, localtuple, remotetuple,
            out HeapTupleData* applytuple, out WorfConflictResolution resolution);

        Conflict.ReportConflict(WorfConflictType.InsertInsert, rel, localtuple, null, remotetuple, applytuple, resolution,
            local.TransactionId, local.CommitTime is not null, local.CommitOrigin, localTs, local.IndexOid,
            relation->trigdesc is not null && relation->trigdesc->trig_insert_before_row);

        if (apply)
        {
            // The row found may not have the remote row's key, so it's changed through its own.
            writer.Update(local.Row, Copy(relation, ModifyTuple(localtuple, relation, newtup)));
        }
    }

    /// <summary>
    /// Handle update via low level api.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="oldtup">The old key.</param>
    /// <param name="newtup">The new tuple.</param>
    internal static void ApplyHeapUpdate(WorfRelation rel, WorfTupleData oldtup, WorfTupleData newtup)
    {
        (PgRelationWriter writer, PgReplicatedRow row, PgReplicatedRow key, uint replicaIndex) = Writer(rel);
        RelationData* relation = rel.Rel;
        CheckReplicaIndex(rel, replicaIndex);
        // Both rows are always filled, so neither call is skipped.
        bool keyReplicated = SetRow(key, oldtup);
        bool rowReplicated = SetRow(row, newtup);
        bool replicated = keyReplicated && rowReplicated;

        // Search for existing tuple with same key
        PgFoundRow? local = replicated ? writer.Find(key) : writer.Find(HeapRow(rel, oldtup));

        if (local is null)
        {
            // The tuple to be updated could not be found.
            //
            // We can't do INSERT here because we might not have whole tuple.
            Conflict.ReportConflict(WorfConflictType.UpdateDelete, rel, null, oldtup, FormTuple(relation, newtup), null,
                WorfConflictResolution.Skip, 0, false, 0, 0, writer.KeyIndexOid, false);
            return;
        }

        bool apply = true;
        long localTs = local.CommitTime?.MicrosecondsSinceEpoch ?? 0;
        HeapTupleData* localtuple = null;

        // If the local tuple was previously updated by different transaction on different server, consider this to be
        // conflict and resolve it.
        if (local.CommitTime is not null &&
            local.TransactionId != NativeMethods.GetTopTransactionId() &&
            local.CommitOrigin != Worf.ReploriginSessionOrigin)
        {
            localtuple = LocalTuple(local);
            HeapTupleData* remotetuple = ModifyTuple(localtuple, relation, newtup);

            apply = Conflict.TryResolveConflict(local.CommitOrigin, localTs, localtuple, remotetuple,
                out HeapTupleData* applytuple, out WorfConflictResolution resolution);

            Conflict.ReportConflict(WorfConflictType.UpdateUpdate, rel, localtuple, oldtup, remotetuple, applytuple,
                resolution, local.TransactionId, true, local.CommitOrigin, localTs, local.IndexOid,
                relation->trigdesc is not null && relation->trigdesc->trig_update_before_row);
        }

        if (!apply)
        {
            return;
        }

        // Columns the update doesn't send, such as unchanged TOAST values, keep their local values.
        if (replicated)
        {
            writer.Update(key, row);
        }
        else
        {
            localtuple = localtuple is not null ? localtuple : LocalTuple(local);
            writer.Update(local.Row, Copy(relation, ModifyTuple(localtuple, relation, newtup)));
        }
    }

    /// <summary>
    /// Handle delete via low level api.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="oldtup">The old key.</param>
    internal static void ApplyHeapDelete(WorfRelation rel, WorfTupleData oldtup)
    {
        (PgRelationWriter writer, _, PgReplicatedRow key, uint replicaIndex) = Writer(rel);
        CheckReplicaIndex(rel, replicaIndex);

        if (!(SetRow(key, oldtup) ? writer.Delete(key) : writer.Delete(HeapRow(rel, oldtup))))
        {
            // The tuple to be deleted could not be found.
            Conflict.ReportConflict(WorfConflictType.DeleteDelete, rel, null, oldtup, FormTuple(rel.Rel, oldtup), null,
                WorfConflictResolution.Skip, 0, false, 0, 0, writer.KeyIndexOid, false);
        }
    }

    /// <summary>
    /// Whether the relation's inserts may be batched.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <returns>Whether conflicts result in errors, which batching requires.</returns>
    internal static bool ApplyHeapCanMi(WorfRelation rel)
    {
        _ = rel;

        // Multi insert is only supported when conflicts result in errors.
        return Conflict.ConflictResolver == WorfResolveOption.Error;
    }

    /// <summary>
    /// Add tuple to the MultiInsert.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="tup">The tuple.</param>
    internal static void ApplyHeapMiAddTuple(WorfRelation rel, WorfTupleData tup)
    {
        (PgRelationWriter writer, PgReplicatedRow row, _, _) = Writer(rel);

        if (SetRow(row, tup))
        {
            writer.InsertBuffered(row);
        }
        else
        {
            writer.InsertBuffered(HeapRow(rel, tup));
        }
    }

    /// <summary>
    /// Finish the MultiInsert.
    /// </summary>
    /// <param name="rel">The relation.</param>
    internal static void ApplyHeapMiFinish(WorfRelation rel)
    {
        if (s_writers.TryGetValue(rel.Reloid, out (PgRelationWriter Writer, PgReplicatedRow Row, PgReplicatedRow Key, uint ReplicaIndex) entry))
        {
            entry.Writer.Flush();
        }
    }

    /// <summary>
    /// Gets the table's writer for the remote transaction, opening it on the first change.
    /// </summary>
    private static (PgRelationWriter Writer, PgReplicatedRow Row, PgReplicatedRow Key, uint ReplicaIndex) Writer(WorfRelation rel)
    {
        if (!s_writers.TryGetValue(rel.Reloid, out (PgRelationWriter Writer, PgReplicatedRow Row, PgReplicatedRow Key, uint ReplicaIndex) entry))
        {
            if (s_writers.Count == MaxWriters)
            {
                _ = s_writers.Remove(s_writerOrder.Dequeue(), out (PgRelationWriter Writer, PgReplicatedRow, PgReplicatedRow, uint) oldest);
                oldest.Writer.Dispose();
            }

            PgRelationWriter writer = PgRelationWriter.Open(rel.Reloid);
            entry = (writer, writer.CreateReplicatedRow(), writer.CreateReplicatedRow(), NativeMethods.RelationGetReplicaIndex(rel.Rel));
            s_writers.Add(rel.Reloid, entry);
            s_writerOrder.Enqueue(rel.Reloid);
        }

        return entry;
    }

    /// <summary>
    /// Updates and deletes find their rows by the replica identity index, which the table needs to have. The writer
    /// would otherwise compare every column.
    /// </summary>
    private static void CheckReplicaIndex(WorfRelation rel, uint replicaIndex)
    {
        if (replicaIndex == 0)
        {
            throw new PgException(PgSqlStates.InternalError,
                $"could not find REPLICA IDENTITY index for table {Worf.FromServer((sbyte*)&rel.Rel->rd_rel->relname)} with oid {rel.Reloid}",
                hint: "The REPLICA IDENTITY index is usually the PRIMARY KEY. See the PostgreSQL docs for ALTER TABLE ... REPLICA IDENTITY");
        }
    }

    /// <summary>
    /// Puts the columns a tuple sends into a row in replication's form. A column it doesn't send, or sends as an
    /// unchanged TOAST value, stays unsent.
    /// </summary>
    /// <returns>Whether the row holds the tuple; a column in PostgreSQL's internal form has no replication form.</returns>
    private static bool SetRow(PgReplicatedRow row, WorfTupleData tuple)
    {
        row.Clear();
        for (int attid = 0; attid < tuple.Kinds.Length; attid++)
        {
            if (!tuple.Changed[attid])
            {
                continue;
            }

            switch (tuple.Kinds[attid])
            {
                case 'n':
                    row.SetNull(attid);
                    break;
                case 't':
                    row.SetText(attid, tuple.Data[attid]);
                    break;
                case 'b':
                    row.SetBinary(attid, tuple.Data[attid]);
                    break;
                default:
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Gets a tuple's values as a row the writer takes, for a tuple with columns in PostgreSQL's internal form.
    /// </summary>
    private static PgHeapTuple HeapRow(WorfRelation rel, WorfTupleData tuple) => Copy(rel.Rel, FormTuple(rel.Rel, tuple));

    private static PgHeapTuple Copy(RelationData* relation, HeapTupleData* tuple)
        => PgHeapTuple.DangerousCopy((nint)tuple, (nint)relation->rd_att);

    private static HeapTupleData* LocalTuple(PgFoundRow local)
        => (HeapTupleData*)local.Row.DangerousToNative(PgMemoryContext.Current);

    private static HeapTupleData* FormTuple(RelationData* relation, WorfTupleData tuple)
    {
        tuple.ConvertValues(relation->rd_att);

        fixed (ulong* values = tuple.Values)
        fixed (bool* nulls = tuple.Nulls)
        {
            return NativeMethods.heap_form_tuple(relation->rd_att, values, nulls);
        }
    }

    private static HeapTupleData* ModifyTuple(HeapTupleData* localtuple, RelationData* relation, WorfTupleData tuple)
    {
        tuple.ConvertValues(relation->rd_att);

        fixed (ulong* values = tuple.Values)
        fixed (bool* nulls = tuple.Nulls)
        fixed (bool* changed = tuple.Changed)
        {
            return NativeMethods.heap_modify_tuple(localtuple, relation->rd_att, values, nulls, changed);
        }
    }
}
