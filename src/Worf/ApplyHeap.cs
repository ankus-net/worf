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

    // The transaction id pglogical reads a frozen row's as, FrozenTransactionId.
    private const uint FrozenTransactionId = 2;

    // HEAP_XMIN_FROZEN, the infomask bits of a tuple header that say its xmin is frozen: HEAP_XMIN_COMMITTED and
    // HEAP_XMIN_INVALID together.
    private const ushort HeapXminFrozen = 0x0300;

    // SK_ISNULL and SK_SEARCHNULL, the flags of a scan key that finds NULLs.
    private const int ScanKeyIsNull = 0x0001;
    private const int ScanKeySearchNull = 0x0040;

    private const int AccessShareLock = 1;
    private const ushort BtEqualStrategyNumber = 3;

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

        // Tuple already exists, try resolving conflict. A frozen one has no commit timestamp or origin.
        RelationData* relation = rel.Rel;
        HeapTupleData* localtuple = LocalTuple(local);
        HeapTupleData* remotetuple = FormTuple(relation, newtup);
        bool frozen = local.CommitTime is not null && IsFrozen(relation, local.IndexOid, localtuple);
        PgTimestampTz? commitTime = frozen ? null : local.CommitTime;
        ushort commitOrigin = frozen ? (ushort)0 : local.CommitOrigin;
        uint localXid = frozen ? FrozenTransactionId : local.TransactionId;
        long localTs = commitTime?.MicrosecondsSinceEpoch ?? 0;

        bool apply = Conflict.TryResolveConflict(commitOrigin, localXid, localTs, localtuple, remotetuple,
            out HeapTupleData* applytuple, out WorfConflictResolution resolution);

        Conflict.ReportConflict(WorfConflictType.InsertInsert, rel, localtuple, null, remotetuple, applytuple, resolution,
            localXid, commitTime is not null, commitOrigin, localTs, local.IndexOid,
            relation->trigdesc is not null && relation->trigdesc->trig_insert_before_row);

        if (apply)
        {
            // The row found may not have the remote row's key, so it's changed through its own.
            writer.Update(local.Row, Copy(relation, ModifyTuple(localtuple, relation, newtup)));
        }
    }

    /// <summary>
    /// Gives a column's value of a tuple as its type's output, in the database's encoding, or null.
    /// </summary>
    private static byte[]? ColumnBytes(RelationData* relation, HeapTupleData* tuple, int attnum)
    {
        bool isnull;
        ulong value = NativeMethods.heap_getattr(tuple, attnum, relation->rd_att, &isnull);
        if (isnull)
        {
            return null;
        }

        uint typoutput;
        bool varlena;
        NativeMethods.getTypeOutputInfo(NativeMethods.TupleDescAttr(relation->rd_att, attnum - 1)->atttypid, &typoutput, &varlena);
        sbyte* text = NativeMethods.OidOutputFunctionCall(typoutput, varlena ? (ulong)NativeMethods.pg_detoast_datum((varlena*)value) : value);
        return new ReadOnlySpan<byte>(text, Worf.StrLen(text)).ToArray();
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
        HeapTupleData* localtuple = null;

        // If the local tuple was previously updated by different transaction on different server, consider this to be
        // conflict and resolve it. One that came through this subscription, from any node its provider relays, came
        // in order, and a frozen one has no commit timestamp or origin, as in pglogical.
        if (local.CommitTime is PgTimestampTz commitTime &&
            local.TransactionId != NativeMethods.GetTopTransactionId() &&
            !Apply.IsOwnStream(local.CommitOrigin, local.TransactionId))
        {
            localtuple = LocalTuple(local);
            if (!IsFrozen(relation, local.IndexOid, localtuple))
            {
                long localTs = commitTime.MicrosecondsSinceEpoch;
                HeapTupleData* remotetuple = ModifyTuple(localtuple, relation, newtup);

                apply = Conflict.TryResolveConflict(local.CommitOrigin, local.TransactionId, localTs, localtuple, remotetuple,
                    out HeapTupleData* applytuple, out WorfConflictResolution resolution);

                Conflict.ReportConflict(WorfConflictType.UpdateUpdate, rel, localtuple, oldtup, remotetuple, applytuple,
                    resolution, local.TransactionId, true, local.CommitOrigin, localTs, local.IndexOid,
                    relation->trigdesc is not null && relation->trigdesc->trig_update_before_row);
            }
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

            PgRelationWriter writer = rel.HasRechecks ? ReplicaRechecks.Firing(rel.Reloid, () => PgRelationWriter.Open(rel.Reloid), true)
                : PgRelationWriter.Open(rel.Reloid);
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
    /// <returns>Whether the row holds the tuple; a column in PostgreSQL's internal form, or converted to a type with
    /// no binary form, has no replication form.</returns>
    private static bool SetRow(PgReplicatedRow row, WorfTupleData tuple)
    {
        row.Clear();
        for (int attid = 0; attid < tuple.Kinds.Length; attid++)
        {
            if (!tuple.Changed[attid])
            {
                continue;
            }

            // A value sent in another type is converted, and goes in the column type's binary form.
            if (tuple.Conversions?[attid] is { } conversion)
            {
                tuple.ConvertFromSentType(attid);
                if (tuple.Nulls[attid])
                {
                    row.SetNull(attid);
                    continue;
                }

                varlena* bytes = conversion.Send(tuple.Values[attid]);
                if (bytes is null)
                {
                    return false;
                }

                row.SetBinary(attid, new ReadOnlySpan<byte>((byte*)bytes + Varlena.Hdrsz, Varlena.Size((byte*)bytes) - Varlena.Hdrsz));
                NativeMethods.pfree(bytes);
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

    /// <summary>
    /// Checks whether a local row is frozen, as its tuple header says once vacuum or <c>COPY FREEZE</c> has frozen it,
    /// which pglogical reads as <c>FrozenTransactionId</c>, with no commit timestamp or origin: the transaction id the
    /// header keeps can be older than PostgreSQL keeps either for. The row found has its values but not its header, so
    /// it's found again through the index it was found through, under a snapshot that sees it, as it's locked.
    /// </summary>
    /// <param name="relation">The row's table.</param>
    /// <param name="indexOid">The unique index the row was found through.</param>
    /// <param name="localtuple">The row's values.</param>
    /// <returns>Whether it's frozen.</returns>
    private static bool IsFrozen(RelationData* relation, uint indexOid, HeapTupleData* localtuple)
    {
        // Rows are found through an index, the table's key for a change's old row or a unique index for a new row.
        if (indexOid == 0)
        {
            return false;
        }

        RelationData* index = NativeMethods.index_open(indexOid, AccessShareLock);
        int nkeys = index->rd_index->indnkeyatts;
        short* indkey = (short*)(&index->rd_index->indkey + 1);
        ScanKeyData* keys = stackalloc ScanKeyData[nkeys];
        bool usable = true;
        for (int i = 0; i < nkeys; i++)
        {
            // Lookups skip indexes on expressions.
            short attno = indkey[i];
            if (attno <= 0)
            {
                usable = false;
                break;
            }

            bool isnull;
            ulong value = NativeMethods.heap_getattr(localtuple, attno, relation->rd_att, &isnull);
            if (isnull)
            {
                // Only an index whose NULLs aren't distinct finds a row by a NULL.
                NativeMethods.ScanKeyEntryInitialize(&keys[i], ScanKeyIsNull | ScanKeySearchNull, attno, 0, 0, 0, 0, 0);
            }
            else
            {
                uint opcintype = index->rd_opcintype[i];
                uint op = NativeMethods.get_opfamily_member(index->rd_opfamily[i], opcintype, opcintype, (short)BtEqualStrategyNumber);
                NativeMethods.ScanKeyInit(&keys[i], attno, BtEqualStrategyNumber, NativeMethods.get_opcode(op), value);
                keys[i].sk_collation = index->rd_indcollation[i];
            }
        }

        NativeMethods.index_close(index, AccessShareLock);
        if (!usable)
        {
            return false;
        }

        // PostgreSQL 18 checks that a scan's snapshot is registered.
        SnapshotData* snapshot = NativeMethods.RegisterSnapshot(NativeMethods.GetLatestSnapshot());
        SysScanDescData* scan = NativeMethods.systable_beginscan(relation, indexOid, true, snapshot, nkeys, keys);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);
        bool frozen = tuple is not null && XminFrozen(tuple->t_data);
        NativeMethods.systable_endscan(scan);
        NativeMethods.UnregisterSnapshot(snapshot);
        return frozen;
    }

    /// <summary>
    /// Checks whether a tuple header says its row is frozen, as <c>HeapTupleHeaderXminFrozen</c> does.
    /// </summary>
    /// <param name="header">The header.</param>
    /// <returns>Whether it does.</returns>
    internal static bool XminFrozen(HeapTupleHeaderData* header) => (header->t_infomask & HeapXminFrozen) == HeapXminFrozen;

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
