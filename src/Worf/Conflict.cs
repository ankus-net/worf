using System.Text;
using Ankus.Postgres;
namespace Worf;

/// <summary>
/// worf conflict detection and resolution.
/// </summary>
internal static partial class Conflict
{
    /// <summary>
    /// Gets the method used for conflict resolution for resolvable conflicts.
    /// </summary>
    [PgGucEnum("worf.conflict_resolution", WorfResolveOption.ApplyRemote,
        "Sets method used for conflict resolution for resolvable conflicts.", Context = PgGucContext.SuperuserSet,
        Check = nameof(ConflictResolverCheckHook))]
    internal static partial WorfResolveOption ConflictResolver { get; }

    /// <summary>
    /// Gets the log level used for logging resolved conflicts.
    /// </summary>
    [PgGucEnum("worf.conflict_log_level", ServerMessageLevel.Log, "Sets log level used for logging resolved conflicts.",
        Context = PgGucContext.SuperuserSet)]
    internal static partial ServerMessageLevel ConflictLogLevel { get; }

    /// <summary>
    /// Checks validity of the <c>worf.conflict_resolution</c> setting. Only allow apply_remote when
    /// track_commit_timestamp is off, because there is no way to know where the local tuple originated from.
    /// </summary>
    /// <param name="newval">The proposed value.</param>
    /// <param name="source">The value's source.</param>
    /// <returns>The accepted value, or the rejection.</returns>
    internal static PgGucCheckResult<WorfResolveOption> ConflictResolverCheckHook(WorfResolveOption newval, PgGucSource source)
    {
        bool trackCommitTimestamp;
        unsafe
        {
            trackCommitTimestamp = NativeGlobals.track_commit_timestamp;
        }

        if (!trackCommitTimestamp && newval != WorfResolveOption.ApplyRemote && newval != WorfResolveOption.Error)
        {
            return new(new PgGucCheckError(detail: "track_commit_timestamp is off"));
        }

        return new(newval);
    }

    private const int MaxConflictLogAttrLen = 40;
    private const int IndexMaxKeys = 32;
    private const short BtEqualStrategyNumber = 3;
    private const int SkIsnull = 0x0001;
    private const uint FirstNormalTransactionId = 3;

    /// <summary>
    /// Find tuple using REPLICA IDENTITY index and output it in <paramref name="oldslot"/> if found.
    /// </summary>
    /// <remarks>The index oid is also output.</remarks>
    /// <param name="relinfo">The relation's result info.</param>
    /// <param name="tuple">The key to look for.</param>
    /// <param name="oldslot">The slot to store the found row in.</param>
    /// <param name="idxrelid">The replica identity index.</param>
    /// <returns>Whether the row was found.</returns>
    internal static unsafe bool TupleFindReplidx(ResultRelInfo* relinfo, WorfTupleData tuple, TupleTableSlot* oldslot,
        out uint idxrelid)
    {
        ScanKeyData* indexKey = stackalloc ScanKeyData[IndexMaxKeys];

        // Open REPLICA IDENTITY index.
        uint idxoid = NativeMethods.RelationGetReplicaIndex(relinfo->ri_RelationDesc);
        if (idxoid == 0)
        {
            uint relid = relinfo->ri_RelationDesc->rd_id;
            throw new PgException(PgSqlStates.InternalError,
                $"could not find REPLICA IDENTITY index for table {Worf.FromServer(NativeMethods.get_rel_name(relid))} with oid {relid}",
                hint: "The REPLICA IDENTITY index is usually the PRIMARY KEY. See the PostgreSQL docs for ALTER TABLE ... REPLICA IDENTITY");
        }

        idxrelid = idxoid;
        RelationData* idxrel = NativeMethods.index_open(idxoid, (int)PgLockMode.RowExclusive);

        // Build scan key for just opened index
        _ = BuildIndexScanKey(indexKey, relinfo->ri_RelationDesc, idxrel, tuple);

        // Try to find the row and store any matching row in 'oldslot'.
        bool found = FindIndexTuple(indexKey, relinfo->ri_RelationDesc, idxrel, LockTupleMode.LockTupleExclusive, oldslot);

        // Don't release lock until commit.
        NativeMethods.index_close(idxrel, (int)PgLockMode.None);

        return found;
    }

    /// <summary>
    /// Find the tuple in a table using any index and returns the conflicting index's oid, if any conflict found.
    /// </summary>
    /// <remarks>
    /// This is not wholly safe. It does not consider the table's upstream replica identity, and may choose to resolve
    /// the conflict on a unique index that isn't part of the replica identity.
    ///
    /// Order-of-apply issues between multiple upstreams can lead to non-deterministic behaviour in cases where we
    /// resolve one conflict using one index, then a second conflict using a different index.
    ///
    /// We should really respect the replica identity more (i.e. use TupleFindReplidx). Or at least raise a WARNING
    /// that an inconsistency may arise.
    /// </remarks>
    /// <param name="relinfo">The relation's result info, with its indexes open.</param>
    /// <param name="tuple">The row to look for.</param>
    /// <param name="outslot">The slot to store the conflicting row in.</param>
    /// <returns>The conflicting index, or 0.</returns>
    internal static unsafe uint TupleFindConflict(ResultRelInfo* relinfo, WorfTupleData tuple, TupleTableSlot* outslot)
    {
        uint conflictIdx = 0;
        ScanKeyData* indexKey = stackalloc ScanKeyData[IndexMaxKeys];
        bool found;

        // Check the replica identity index with a SnapshotDirty scan first, like TupleFindReplidx, but without
        // ERRORing if we don't find a replica identity index.
        uint replidxoid = NativeMethods.RelationGetReplicaIndex(relinfo->ri_RelationDesc);
        if (replidxoid != 0)
        {
            ScanKeyData* indexKey1 = stackalloc ScanKeyData[IndexMaxKeys];
            RelationData* replidxrel = NativeMethods.index_open(replidxoid, (int)PgLockMode.RowExclusive);
            _ = BuildIndexScanKey(indexKey1, relinfo->ri_RelationDesc, replidxrel, tuple);
            found = FindIndexTuple(indexKey1, relinfo->ri_RelationDesc, replidxrel, LockTupleMode.LockTupleExclusive,
                outslot);
            NativeMethods.index_close(replidxrel, (int)PgLockMode.None);
            if (found)
            {
                return replidxoid;
            }
        }

        // Do a SnapshotDirty search for conflicting tuples. If any is found store it in outslot and return the oid of
        // the matching index. We don't continue scanning for matches in other indexes, so we won't notice if the
        // tuple conflicts with another index, and it'll raise a unique violation on apply instead.
        //
        // We could carry on here even if (found) and look for secondary conflicts, but all we'd be able to do would
        // be ERROR here instead of later. The rest of the time we'd just pay a useless performance cost for extra
        // index scans.
        for (int i = 0; i < relinfo->ri_NumIndices; i++)
        {
            IndexInfo* ii = relinfo->ri_IndexRelationInfo[i];

            // Only unique indexes are of interest here, and we can't deal with expression indexes so far.
            if (!ii->ii_Unique || ii->ii_Expressions is not null)
            {
                continue;
            }

            // TODO: predicates should be handled better. There's no point scanning an index where the predicates
            // show it could never match anyway, and it can produce false conflicts if the predicate includes
            // non-indexed columns. We could find a local tuple that matches the predicate in the index, but there's
            // only a true conflict if the remote tuple also matches the predicate. If we ignore the predicate we
            // generate a false conflict. See RM#1839.
            //
            // For now we reject conflict resolution on indexes with predicates entirely. If there's a conflict it'll
            // be raised on apply with a unique violation.
            if (ii->ii_Predicate is not null)
            {
                continue;
            }

            RelationData* idxrel = relinfo->ri_IndexRelationDescs[i];

            // No point re-scanning the replica identity index
            if (idxrel->rd_id == replidxoid)
            {
                continue;
            }

            // If this index may not be complete enough to exhibit uniqueness and drive correct query results, move
            // on to other indexes. If no index meets all qualifications and this one is ii_ReadyForInserts, one could
            // argue for using this one instead of using no index. We don't offer that. (The server's
            // infer_arbiter_indexes(), which performs a similar role, also requires indisvalid.)
            if (!idxrel->rd_index->indisvalid)
            {
                continue;
            }

            if (BuildIndexScanKey(indexKey, relinfo->ri_RelationDesc, idxrel, tuple))
            {
                continue;
            }

            // Try to find conflicting row and store in 'outslot'
            found = FindIndexTuple(indexKey, relinfo->ri_RelationDesc, idxrel, LockTupleMode.LockTupleExclusive,
                outslot);

            if (found)
            {
                conflictIdx = idxrel->rd_id;
                break;
            }

            PgInterrupts.Check();
        }

        return conflictIdx;
    }

    /// <summary>
    /// Get the origin of the local tuple.
    /// </summary>
    /// <remarks>
    /// If the track_commit_timestamp is off, we return remote origin info since there is no way to get any
    /// meaningful info locally. This means that the caller will assume that all the local tuples came from remote
    /// site when track_commit_timestamp is off.
    ///
    /// This function is used by UPDATE conflict detection so the above means that UPDATEs will not be recognized as
    /// conflict even if they change locally modified row.
    /// </remarks>
    /// <param name="localTuple">The local row.</param>
    /// <param name="xmin">The row's xmin.</param>
    /// <param name="localOrigin">The origin that wrote the row.</param>
    /// <param name="localTs">When the row's transaction committed.</param>
    /// <returns>Whether local origin data was found.</returns>
    internal static unsafe bool GetTupleOrigin(HeapTupleData* localTuple, out uint xmin, out ushort localOrigin,
        out long localTs)
    {
        xmin = NativeMethods.HeapTupleHeaderGetXmin(localTuple->t_data);
        if (!NativeGlobals.track_commit_timestamp)
        {
            localOrigin = Worf.ReploriginSessionOrigin;
            localTs = Worf.ReploriginSessionOriginTimestamp;
            return false;
        }
        else
        {
            if (xmin != 0 && xmin < FirstNormalTransactionId)
            {
                // Pg emits an ERROR if you try to pass FrozenTransactionId (2) or BootstrapTransactionId (1) to
                // TransactionIdGetCommitTsData, per RT#46983 . This seems like an oversight in the core function, but
                // we can work around it here by setting it to the same thing we'd get if the xid's commit timestamp
                // was trimmed already.
                localOrigin = 0;
                localTs = 0;
                return false;
            }
            else
            {
                long ts;
                ushort origin;
                bool found = NativeMethods.TransactionIdGetCommitTsData(xmin, &ts, &origin);
                localTs = ts;
                localOrigin = origin;
                return found;
            }
        }
    }

    /// <summary>
    /// Try resolving the conflict resolution.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="localtuple">The local row.</param>
    /// <param name="remotetuple">The remote row.</param>
    /// <param name="resulttuple">The row to keep.</param>
    /// <param name="resolution">How the conflict was resolved.</param>
    /// <returns>Whether the remote tuple should be applied.</returns>
    internal static unsafe bool TryResolveConflict(RelationData* rel, HeapTupleData* localtuple,
        HeapTupleData* remotetuple, out HeapTupleData* resulttuple, out WorfConflictResolution resolution)
    {
        _ = rel;
        bool apply;
        ushort localOrigin;
        long localTs;

        switch (ConflictResolver)
        {
            case WorfResolveOption.Error:
                // TODO: proper error message
                throw new PgException(PgSqlStates.InternalError, "cannot apply conflicting row");

            case WorfResolveOption.ApplyRemote:
                apply = true;
                resolution = WorfConflictResolution.ApplyRemote;
                break;

            case WorfResolveOption.KeepLocal:
                apply = false;
                resolution = WorfConflictResolution.KeepLocal;
                break;

            case WorfResolveOption.LastUpdateWins:
                _ = GetTupleOrigin(localtuple, out _, out localOrigin, out localTs);
                apply = ConflictResolveByTimestamp(localOrigin, Worf.ReploriginSessionOrigin, localTs,
                    Worf.ReploriginSessionOriginTimestamp, true, out resolution);
                break;

            case WorfResolveOption.FirstUpdateWins:
                _ = GetTupleOrigin(localtuple, out _, out localOrigin, out localTs);
                apply = ConflictResolveByTimestamp(localOrigin, Worf.ReploriginSessionOrigin, localTs,
                    Worf.ReploriginSessionOriginTimestamp, false, out resolution);
                break;

            default:
                throw new PgException(PgSqlStates.InternalError, $"unrecognized worf_conflict_resolver setting {(int)ConflictResolver}");
        }

        resulttuple = apply ? remotetuple : localtuple;

        return apply;
    }

    /// <summary>
    /// Gets a resolution's name.
    /// </summary>
    /// <param name="resolution">The resolution.</param>
    /// <returns>The name.</returns>
    internal static string ConflictResolutionToString(WorfConflictResolution resolution) => resolution switch
    {
        WorfConflictResolution.ApplyRemote => "apply_remote",
        WorfConflictResolution.KeepLocal => "keep_local",
        WorfConflictResolution.Skip => "skip",
        _ => "unknown",
    };

    /// <summary>
    /// Log the conflict to server log.
    /// </summary>
    /// <remarks>
    /// There are number of tuples passed:
    /// - The local tuple we conflict with or NULL if not found [localtuple];
    /// - If the remote tuple was an update, the key of the old tuple [oldkey]
    /// - The remote tuple, after we fill any defaults and apply any local BEFORE triggers but before conflict
    ///   resolution [remotetuple];
    /// - The tuple we'll actually apply if any, after conflict resolution [applytuple].
    /// </remarks>
    internal static unsafe void ReportConflict(WorfConflictType conflictType, WorfRelation rel, HeapTupleData* localtuple,
        WorfTupleData? oldkey, HeapTupleData* remotetuple, HeapTupleData* applytuple, WorfConflictResolution resolution,
        uint localTupleXid, bool foundLocalOrigin, ushort localTupleOrigin, long localTupleCommitTs, uint conflictIdxOid,
        bool hasBeforeTriggers)
    {
        _ = oldkey;
        _ = applytuple;
        RelationData* relation = rel.Rel;
        TupleDescData* desc = relation->rd_att;
        string localTupTsStr = "";
        string idxname = "(unknown)";

        if (foundLocalOrigin)
        {
            localTupTsStr = Worf.FromServer(NativeMethods.timestamptz_to_str(localTupleCommitTs));
        }

        string remotetup = TupleToString(desc, remotetuple);
        string localtup = localtuple is not null ? TupleToString(desc, localtuple) : "";

        if (conflictIdxOid != 0)
        {
            sbyte* name = NativeMethods.get_rel_name(conflictIdxOid);
            idxname = name is null ? null! : Worf.FromServer(name);
        }

        string qualrelname = Spi.QuoteQualifiedIdentifier(
            Worf.FromServer(NativeMethods.get_namespace_name(relation->rd_rel->relnamespace)),
            Worf.FromServer((sbyte*)&relation->rd_rel->relname));

        ushort originId = Worf.ReploriginSessionOrigin;
        string originTs = Worf.FromServer(NativeMethods.timestamptz_to_str(Worf.ReploriginSessionOriginTimestamp));
        ulong originLsn = Worf.ReploriginSessionOriginLsn;

        // pglogical formats the commit position's high half from a 32-bit shift, which always gives zero.
        string commitLsn = $"{0:X}/{(uint)originLsn:X}";

        // We try to provide a lot of information about conflicting tuples because the conflicts are often transient
        // and timing-sensitive. It's rare that we can examine a stopped system or reproduce them at leisure. So the
        // more info we have in the logs, the better chance we have of diagnosing application issues. It's worth
        // paying the price of some log spam.
        switch (conflictType)
        {
            case WorfConflictType.InsertInsert:
            case WorfConflictType.UpdateUpdate:
                PgLog.Write(ToPgLogLevel(ConflictLogLevel), new PgDiagnostic(
                    $"CONFLICT: remote {(conflictType == WorfConflictType.InsertInsert ? "INSERT" : "UPDATE")} on relation {qualrelname} (local index {idxname}). Resolution: {ConflictResolutionToString(resolution)}.")
                {
                    SqlState = PgSqlStates.IntegrityConstraintViolation,
                    Detail = $"existing local tuple {{{localtup}}} xid={localTupleXid},origin={(foundLocalOrigin ? localTupleOrigin : -1)},timestamp={localTupTsStr}; remote tuple {{{remotetup}}}{(hasBeforeTriggers ? "*" : "")} in xact origin={originId},timestamp={originTs},commit_lsn={commitLsn}",
                });
                break;

            case WorfConflictType.UpdateDelete:
            case WorfConflictType.DeleteDelete:
                PgLog.Write(ToPgLogLevel(ConflictLogLevel), new PgDiagnostic(
                    $"CONFLICT: remote {(conflictType == WorfConflictType.UpdateDelete ? "UPDATE" : "DELETE")} on relation {qualrelname} replica identity index {idxname} (tuple not found). Resolution: {ConflictResolutionToString(resolution)}.")
                {
                    SqlState = PgSqlStates.IntegrityConstraintViolation,
                    Detail = $"remote tuple {{{remotetup}}}{(hasBeforeTriggers ? "*" : "")} in xact origin={originId},timestamp={originTs},commit_lsn={commitLsn}",
                });
                break;

            default:
                break;
        }

        // Record conflict to history table if enabled
        ConflictHistory.RecordConflict(conflictType, rel, localtuple, remotetuple, resolution, localTupleXid,
            foundLocalOrigin, localTupleOrigin, localTupleCommitTs, conflictIdxOid, hasBeforeTriggers);
    }

    /// <summary>
    /// Maps a setting's message level to a log level.
    /// </summary>
    /// <param name="level">The message level.</param>
    /// <returns>The log level.</returns>
    internal static PgLogLevel ToPgLogLevel(ServerMessageLevel level) => level switch
    {
        ServerMessageLevel.Debug5 => PgLogLevel.Debug5,
        ServerMessageLevel.Debug4 => PgLogLevel.Debug4,
        ServerMessageLevel.Debug3 => PgLogLevel.Debug3,
        ServerMessageLevel.Debug2 => PgLogLevel.Debug2,
        ServerMessageLevel.Debug1 => PgLogLevel.Debug1,
        ServerMessageLevel.Info => PgLogLevel.Info,
        ServerMessageLevel.Notice => PgLogLevel.Notice,
        ServerMessageLevel.Warning => PgLogLevel.Warning,
        ServerMessageLevel.Error => PgLogLevel.Error,
        ServerMessageLevel.Fatal => PgLogLevel.Fatal,
        ServerMessageLevel.Panic => PgLogLevel.Panic,
        _ => PgLogLevel.Log,
    };

    /// <summary>
    /// Gets a column value's text for logging, as <c>tuple_to_stringinfo</c> and the conflict history read it.
    /// </summary>
    /// <param name="desc">The descriptor.</param>
    /// <param name="tuple">The tuple.</param>
    /// <param name="natt">The column's index.</param>
    /// <param name="isnull">Whether the column is null.</param>
    /// <returns>The value's text, or null when the column is null.</returns>
    internal static unsafe string? ColumnOutput(TupleDescData* desc, HeapTupleData* tuple, int natt, out bool isnull)
    {
        FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(desc, natt);
        uint typoutput;
        bool typisvarlena;
        bool attisnull;

        // query output function
        NativeMethods.getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);

        // get Datum from tuple
        ulong origval = NativeMethods.heap_getattr(tuple, natt + 1, desc, &attisnull);
        isnull = attisnull;

        if (attisnull)
        {
            return null;
        }
        else if (typisvarlena && Varlena.IsExternalOndisk(origval))
        {
            return "(unchanged-toast-datum)";
        }

        ulong val = typisvarlena ? (ulong)NativeMethods.pg_detoast_datum((varlena*)origval) : origval;
        sbyte* outputstr = NativeMethods.OidOutputFunctionCall(typoutput, val);
        string result = Worf.FromServer(outputstr);
        NativeMethods.pfree(outputstr);
        return result;
    }

    /// <summary>
    /// Setup a ScanKey for a search in the relation <paramref name="rel"/> for a tuple <paramref name="tup"/> that is
    /// setup to match <paramref name="rel"/> (*NOT* idxrel!).
    /// </summary>
    /// <remarks>
    /// The index's key columns, operator classes and collations come from its relcache entry, which holds what
    /// pglogical reads from the index's pg_index tuple.
    /// </remarks>
    /// <returns>Whether any column in the passed tuple contains a NULL for an indexed field.</returns>
    private static unsafe bool BuildIndexScanKey(ScanKeyData* skey, RelationData* rel, RelationData* idxrel,
        WorfTupleData tup)
    {
        bool hasnulls = false;
        short* indkey = (short*)(&idxrel->rd_index->indkey + 1);

        tup.ConvertValues(rel->rd_att);

        // Examine each indexed attribute to ensure the passed tuple's matching value isn't NULL and we have an
        // equality operator for it.
        for (int attoff = 0; attoff < idxrel->rd_index->indnkeyatts; attoff++)
        {
            short pkattno = (short)(attoff + 1);
            int mainattno = indkey[attoff];
            uint atttype = NativeMethods.attnumTypeId(rel, mainattno);
            uint optype = idxrel->rd_opcintype[attoff];
            uint opfamily = idxrel->rd_opfamily[attoff];

            uint op = NativeMethods.get_opfamily_member(opfamily, optype, optype, BtEqualStrategyNumber);

            if (op == 0)
            {
                throw new PgException(PgSqlStates.InternalError,
                    $"could not lookup equality operator for type {atttype}, optype {optype} in opfamily {opfamily}");
            }

            uint regop = NativeMethods.get_opcode(op);

            // FIXME: convert type?
            NativeMethods.ScanKeyInit(&skey[attoff], pkattno, (ushort)BtEqualStrategyNumber, regop,
                tup.Values[mainattno - 1]);

            skey[attoff].sk_collation = idxrel->rd_indcollation[attoff];

            if (tup.Nulls[mainattno - 1])
            {
                hasnulls = true;
                skey[attoff].sk_flags |= SkIsnull;
            }
        }

        return hasnulls;
    }

    /// <summary>
    /// Search the index <paramref name="idxrel"/> for a tuple identified by <paramref name="skey"/> in
    /// <paramref name="rel"/>.
    /// </summary>
    /// <returns>
    /// If a matching tuple is found lock it with lockmode, fill the slot with its contents and return true, false is
    /// returned otherwise.
    /// </returns>
    private static unsafe bool FindIndexTuple(ScanKeyData* skey, RelationData* rel, RelationData* idxrel,
        LockTupleMode lockmode, TupleTableSlot* slot)
    {
        bool found;
        int nkeys = idxrel->rd_index->indnkeyatts;

        // We need SnapshotDirty because we're doing uniqueness lookups that must consider rows added/updated by
        // concurrent transactions, just like a normal UNIQUE check does.
        SnapshotData snap = default;
        snap.snapshot_type = SnapshotType.SNAPSHOT_DIRTY;
#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
        IndexScanDescData* scan = NativeMethods.index_beginscan(rel, idxrel, &snap, nkeys, 0);
#elif ANKUS_PG18
        IndexScanDescData* scan = NativeMethods.index_beginscan(rel, idxrel, &snap, null, nkeys, 0);
#else
        IndexScanDescData* scan = NativeMethods.index_beginscan(rel, idxrel, &snap, null, nkeys, 0, 0 /* SO_NONE */);
#endif

        while (true)
        {
            found = false;

            NativeMethods.index_rescan(scan, skey, nkeys, null, 0);

            if (NativeMethods.index_getnext_slot(scan, ScanDirection.ForwardScanDirection, slot))
            {
                found = true;
                NativeMethods.ExecMaterializeSlot(slot);

                // Did any concurrent txn affect the tuple? (See HeapTupleSatisfiesDirty for how we get this).
                uint xwait = snap.xmin != 0 ? snap.xmin : snap.xmax;

                if (xwait != 0)
                {
                    // Wait for the specified transaction to commit or abort
                    NativeMethods.XactLockTableWait(xwait, null, null, XLTW_Oper.XLTW_None);
                    continue;
                }
            }

            // Matching tuple found, no concurrent txns modifying it
            if (found)
            {
                TM_FailureData tmfd;

                NativeMethods.PushActiveSnapshot(NativeMethods.GetLatestSnapshot());

                TM_Result res = NativeMethods.table_tuple_lock(rel, &slot->tts_tid, NativeMethods.GetLatestSnapshot(),
                    slot, NativeMethods.GetCurrentCommandId(false), lockmode, LockWaitPolicy.LockWaitBlock,
                    0 /* don't follow updates */, &tmfd);

                NativeMethods.PopActiveSnapshot();

                switch (res)
                {
                    case TM_Result.TM_Ok:
                        // lock was successfully acquired
                        break;

                    case TM_Result.TM_Updated:
                        // We lost a race between when we looked up the tuple and checked for concurrent modifying
                        // txns and when we tried to lock the matched tuple.
                        //
                        // XXX: Improve handling here.
                        PgLog.Log(new PgDiagnostic("concurrent update, retrying") { SqlState = PgSqlStates.SerializationFailure });
                        continue;

                    default:
                        throw new PgException(PgSqlStates.InternalError, $"unexpected HTSU_Result after locking: {(uint)res}");
                }
            }

            break;
        }

        NativeMethods.index_endscan(scan);

        return found;
    }

    /// <summary>
    /// Resolve conflict based on commit timestamp.
    /// </summary>
    private static bool ConflictResolveByTimestamp(ushort localOriginId, ushort remoteOriginId, long localTs, long remoteTs,
        bool lastUpdateWins, out WorfConflictResolution resolution)
    {
        _ = localOriginId;
        _ = remoteOriginId;
        int cmp = remoteTs.CompareTo(localTs);

        // The logic bellow assumes last update wins, we invert the logic by inverting result of timestamp comparison
        // if first update wins was requested.
        if (!lastUpdateWins)
        {
            cmp = -cmp;
        }

        if (cmp > 0)
        {
            // The remote row wins, update the local one.
            resolution = WorfConflictResolution.ApplyRemote;
            return true;
        }
        else if (cmp < 0)
        {
            // The local row wins, retain it
            resolution = WorfConflictResolution.KeepLocal;
            return false;
        }
        else
        {
            // The timestamps were equal, break the tie in a manner that is consistent across all nodes.
            //
            // XXX: TODO, for now we just always apply remote change.
            resolution = WorfConflictResolution.ApplyRemote;
            return true;
        }
    }

    /// <summary>
    /// print the tuple 'tuple' into a string.
    /// </summary>
    private static unsafe string TupleToString(TupleDescData* tupdesc, HeapTupleData* tuple)
    {
        StringBuilder s = new();
        bool first = true;

        // print all columns individually
        for (int natt = 0; natt < tupdesc->natts; natt++)
        {
            FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(tupdesc, natt);

            // don't print dropped columns, we can't be sure everything is available for them
            if (attr->attisdropped)
            {
                continue;
            }

            // Don't print system columns
            if (attr->attnum < 0)
            {
                continue;
            }

            // print attribute name
            if (first)
            {
                first = false;
            }
            else
            {
                s.Append(' ');
            }

            s.Append(Worf.FromServer((sbyte*)&attr->attname));

            // print attribute type
            HeapTupleData* typeTuple = NativeMethods.typeidType(attr->atttypid);
            s.Append('[');
            s.Append(Worf.FromServer(NativeMethods.typeTypeName(typeTuple)));
            s.Append(']');
            NativeMethods.ReleaseSysCache(typeTuple);

            // print data
            string outputstr = ColumnOutput(tupdesc, tuple, natt, out bool isnull) ?? "(null)";
            _ = isnull;

            // Abbreviate the Datum if it's too long. This may make it syntatically invalid, but it's not like we're
            // writing out a valid ROW(...) as it is.
            byte[] bytes = Worf.ToServer(outputstr);
            if (bytes.Length > MaxConflictLogAttrLen)
            {
                outputstr = Worf.FromServer(bytes.AsSpan(0, MaxConflictLogAttrLen - 5)) + "...";
            }

            s.Append(':');
            s.Append(outputstr);
        }

        return s.ToString();
    }
}
