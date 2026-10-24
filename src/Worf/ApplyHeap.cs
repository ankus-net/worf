using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf apply functions using heap api.
/// </summary>
internal static unsafe class ApplyHeap
{
    private static ApplyMIState? s_pglmistate;

    /// <summary>
    /// Handle the start of a remote transaction.
    /// </summary>
    internal static void ApplyHeapBegin()
    {
    }

    /// <summary>
    /// Handle the end of a remote transaction.
    /// </summary>
    internal static void ApplyHeapCommit()
    {
    }

    /// <summary>
    /// Handle insert via low level api.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="newtup">The new tuple.</param>
    internal static void ApplyHeapInsert(WorfRelation rel, WorfTupleData newtup)
    {
        RelationData* relation = Relation(rel);
        List* recheckIndexes = null;
        bool hasBeforeTriggers = false;

        // The values live as long as the tuple, as they do when pglogical reads it.
        newtup.ConvertValues(relation->rd_att);

        // Initialize the executor state.
        ApplyExecState aestate = InitApplyExecState(rel);
        TupleTableSlot* localslot = NativeMethods.table_slot_create(relation, &aestate.Estate->es_tupleTable);

        NativeMethods.ExecOpenIndices(aestate.ResultRelInfo, false);

        // Check for existing tuple with same key in any unique index containing only normal columns. This doesn't
        // just check the replica identity index, but it'll prefer it and use it first.
        uint conflictsIdxId = Conflict.TupleFindConflict(aestate.ResultRelInfo, newtup, localslot);

        // Process and store remote tuple in the slot
        MemoryContextData* oldctx = Worf.MemoryContextSwitchTo(GetPerTupleMemoryContext(aestate.Estate));
        FillMissingDefaults(rel, aestate.Estate, newtup);
        HeapTupleData* remotetuple = FormTuple(relation, newtup);
        _ = Worf.MemoryContextSwitchTo(oldctx);
        _ = NativeMethods.ExecStoreHeapTuple(remotetuple, aestate.Slot, true);

        if (aestate.ResultRelInfo->ri_TrigDesc is not null &&
            aestate.ResultRelInfo->ri_TrigDesc->trig_insert_before_row)
        {
            hasBeforeTriggers = true;

            if (!NativeMethods.ExecBRInsertTriggers(aestate.Estate, aestate.ResultRelInfo, aestate.Slot))
            {
                FinishApplyExecState(aestate);
                return;
            }
        }

        // trigger might have changed tuple
        remotetuple = NativeMethods.ExecFetchSlotHeapTuple(aestate.Slot, true, null);

        // Did we find matching key in any candidate-key index?
        if (conflictsIdxId != 0)
        {
            bool localOriginFound = Conflict.GetTupleOrigin(TtsTup(localslot), out uint xmin, out ushort localOrigin,
                out long localTs);

            // Tuple already exists, try resolving conflict.
            bool apply = Conflict.TryResolveConflict(relation, TtsTup(localslot), remotetuple,
                out HeapTupleData* applytuple, out WorfConflictResolution resolution);

            Conflict.ReportConflict(WorfConflictType.InsertInsert, rel, TtsTup(localslot), null, remotetuple,
                applytuple, resolution, xmin, localOriginFound, localOrigin, localTs, conflictsIdxId,
                hasBeforeTriggers);

            if (apply)
            {
                if (applytuple != remotetuple)
                {
                    _ = NativeMethods.ExecStoreHeapTuple(applytuple, aestate.Slot, false);
                }

                if (aestate.ResultRelInfo->ri_TrigDesc is not null &&
                    aestate.ResultRelInfo->ri_TrigDesc->trig_update_before_row)
                {
                    if (!ExecBRUpdateTriggers(aestate, &TtsTup(localslot)->t_self))
                    {
                        FinishApplyExecState(aestate);
                        return;
                    }
                }

                // trigger might have changed tuple
                _ = NativeMethods.ExecFetchSlotHeapTuple(aestate.Slot, true, null);

                // Compute stored generated columns
                ComputeGenerated(aestate, CmdType.CMD_UPDATE);

                // Check the constraints of the tuple
                if (relation->rd_att->constr is not null)
                {
                    NativeMethods.ExecConstraints(aestate.ResultRelInfo, aestate.Slot, aestate.Estate);
                }

                if (SimpleTableTupleUpdate(relation, &localslot->tts_tid, aestate))
                {
                    recheckIndexes = UserTableUpdateOpenIndexes(aestate.ResultRelInfo, aestate.Estate, aestate.Slot,
                        true);
                }

                // AFTER ROW UPDATE Triggers
                ExecARUpdateTriggers(aestate, &TtsTup(localslot)->t_self, recheckIndexes);
            }
        }
        else
        {
            // Compute stored generated columns
            ComputeGenerated(aestate, CmdType.CMD_INSERT);

            // Check the constraints of the tuple
            if (relation->rd_att->constr is not null)
            {
                NativeMethods.ExecConstraints(aestate.ResultRelInfo, aestate.Slot, aestate.Estate);
            }

            NativeMethods.simple_table_tuple_insert(aestate.ResultRelInfo->ri_RelationDesc, aestate.Slot);
            _ = UserTableUpdateOpenIndexes(aestate.ResultRelInfo, aestate.Estate, aestate.Slot, false);

            // AFTER ROW INSERT Triggers
            NativeMethods.ExecARInsertTriggers(aestate.Estate, aestate.ResultRelInfo, aestate.Slot, recheckIndexes,
                null);
        }

        FinishApplyExecState(aestate);

        NativeMethods.CommandCounterIncrement();
    }

    /// <summary>
    /// Handle update via low level api.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="oldtup">The old key.</param>
    /// <param name="newtup">The new tuple.</param>
    internal static void ApplyHeapUpdate(WorfRelation rel, WorfTupleData oldtup, WorfTupleData newtup)
    {
        RelationData* relation = Relation(rel);
        List* recheckIndexes = null;
        bool hasBeforeTriggers = false;
        HeapTupleData* remotetuple;

        // The values live as long as the tuples, as they do when pglogical reads them.
        oldtup.ConvertValues(relation->rd_att);
        newtup.ConvertValues(relation->rd_att);

        // Initialize the executor state.
        ApplyExecState aestate = InitApplyExecState(rel);
        TupleTableSlot* localslot = NativeMethods.table_slot_create(relation, &aestate.Estate->es_tupleTable);

        // Search for existing tuple with same key
        bool found = Conflict.TupleFindReplidx(aestate.ResultRelInfo, oldtup, localslot, out uint replidentIdxId);

        // Tuple found, update the local tuple.
        //
        // Note this will fail if there are other unique indexes and one or more of them would be violated by the new
        // tuple.
        if (found)
        {
            bool apply;
            HeapTupleData* applytuple;

            // Process and store remote tuple in the slot
            MemoryContextData* oldctx = Worf.MemoryContextSwitchTo(GetPerTupleMemoryContext(aestate.Estate));
            FillMissingDefaults(rel, aestate.Estate, newtup);
            remotetuple = ModifyTuple(TtsTup(localslot), relation, newtup);
            _ = Worf.MemoryContextSwitchTo(oldctx);
            _ = NativeMethods.ExecStoreHeapTuple(remotetuple, aestate.Slot, true);

            if (aestate.ResultRelInfo->ri_TrigDesc is not null &&
                aestate.ResultRelInfo->ri_TrigDesc->trig_update_before_row)
            {
                hasBeforeTriggers = true;

                if (!ExecBRUpdateTriggers(aestate, &TtsTup(localslot)->t_self))
                {
                    FinishApplyExecState(aestate);
                    return;
                }
            }

            // trigger might have changed tuple
            remotetuple = NativeMethods.ExecFetchSlotHeapTuple(aestate.Slot, true, null);
            bool localOriginFound = Conflict.GetTupleOrigin(TtsTup(localslot), out uint xmin, out ushort localOrigin,
                out long localTs);

            // If the local tuple was previously updated by different transaction on different server, consider this
            // to be conflict and resolve it.
            if (localOriginFound &&
                xmin != NativeMethods.GetTopTransactionId() &&
                localOrigin != Worf.ReploriginSessionOrigin)
            {
                apply = Conflict.TryResolveConflict(relation, TtsTup(localslot), remotetuple, out applytuple,
                    out WorfConflictResolution resolution);

                Conflict.ReportConflict(WorfConflictType.UpdateUpdate, rel, TtsTup(localslot), oldtup, remotetuple,
                    applytuple, resolution, xmin, localOriginFound, localOrigin, localTs, replidentIdxId,
                    hasBeforeTriggers);

                if (applytuple != remotetuple)
                {
                    _ = NativeMethods.ExecStoreHeapTuple(applytuple, aestate.Slot, false);
                }
            }
            else
            {
                apply = true;
            }

            if (apply)
            {
                // Compute stored generated columns
                ComputeGenerated(aestate, CmdType.CMD_UPDATE);

                // Check the constraints of the tuple
                if (relation->rd_att->constr is not null)
                {
                    NativeMethods.ExecConstraints(aestate.ResultRelInfo, aestate.Slot, aestate.Estate);
                }

                if (SimpleTableTupleUpdate(relation, &localslot->tts_tid, aestate))
                {
                    NativeMethods.ExecOpenIndices(aestate.ResultRelInfo, false);
                    recheckIndexes = UserTableUpdateOpenIndexes(aestate.ResultRelInfo, aestate.Estate, aestate.Slot,
                        true);
                }

                // AFTER ROW UPDATE Triggers
                ExecARUpdateTriggers(aestate, &TtsTup(localslot)->t_self, recheckIndexes);
            }
        }
        else
        {
            // The tuple to be updated could not be found.
            //
            // We can't do INSERT here because we might not have whole tuple.
            remotetuple = FormTuple(relation, newtup);
            Conflict.ReportConflict(WorfConflictType.UpdateDelete, rel, null, oldtup, remotetuple, null,
                WorfConflictResolution.Skip, 0, false, 0, 0, replidentIdxId, hasBeforeTriggers);
        }

        // Cleanup.
        FinishApplyExecState(aestate);

        NativeMethods.CommandCounterIncrement();
    }

    /// <summary>
    /// Handle delete via low level api.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="oldtup">The old key.</param>
    internal static void ApplyHeapDelete(WorfRelation rel, WorfTupleData oldtup)
    {
        RelationData* relation = Relation(rel);
        bool hasBeforeTriggers = false;

        // The values live as long as the tuple, as they do when pglogical reads it.
        oldtup.ConvertValues(relation->rd_att);

        // Initialize the executor state.
        ApplyExecState aestate = InitApplyExecState(rel);
        TupleTableSlot* localslot = NativeMethods.table_slot_create(relation, &aestate.Estate->es_tupleTable);

        if (Conflict.TupleFindReplidx(aestate.ResultRelInfo, oldtup, localslot, out uint replidentIdxId))
        {
            if (aestate.ResultRelInfo->ri_TrigDesc is not null &&
                aestate.ResultRelInfo->ri_TrigDesc->trig_delete_before_row)
            {
                bool dodelete = ExecBRDeleteTriggers(aestate, &TtsTup(localslot)->t_self);

                hasBeforeTriggers = true;

                if (!dodelete)
                {
                    // "do nothing"
                    FinishApplyExecState(aestate);
                    return;
                }
            }

            // Tuple found, delete it.
            NativeMethods.simple_heap_delete(relation, &TtsTup(localslot)->t_self);

            // AFTER ROW DELETE Triggers
            ExecARDeleteTriggers(aestate, &TtsTup(localslot)->t_self);
        }
        else
        {
            // The tuple to be deleted could not be found.
            HeapTupleData* remotetuple = FormTuple(relation, oldtup);
            Conflict.ReportConflict(WorfConflictType.DeleteDelete, rel, null, oldtup, remotetuple, null,
                WorfConflictResolution.Skip, 0, false, 0, 0, replidentIdxId, hasBeforeTriggers);
        }

        // Cleanup.
        FinishApplyExecState(aestate);

        NativeMethods.CommandCounterIncrement();
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
        RelationData* relation = Relation(rel);

        // The values live as long as the tuple, as they do when pglogical reads it.
        tup.ConvertValues(relation->rd_att);

        ApplyHeapMiStart(rel);
        ApplyMIState mistate = s_pglmistate!;

        // If sufficient work is pending, process that first
        if (mistate.NbufferedTuples >= mistate.MaxbufferedTuples)
        {
            ApplyHeapMiFlush();
        }

        // Process and store remote tuple in the slot
        ApplyExecState aestate = mistate.Aestate;

        if (mistate.NbufferedTuples == 0)
        {
            // Reset the per-tuple exprcontext. We can only do this if the tuple buffer is empty. (Calling the context
            // the per-tuple memory context is a bit of a misnomer now.)
            if (aestate.Estate->es_per_tuple_exprcontext is not null)
            {
                NativeMethods.MemoryContextReset(aestate.Estate->es_per_tuple_exprcontext->ecxt_per_tuple_memory);
            }
        }

        MemoryContextData* oldctx = Worf.MemoryContextSwitchTo(GetPerTupleMemoryContext(aestate.Estate));
        FillMissingDefaults(rel, aestate.Estate, tup);
        HeapTupleData* remotetuple = FormTuple(relation, tup);
        _ = Worf.MemoryContextSwitchTo(NativeGlobals.TopTransactionContext);
        TupleTableSlot* slot = aestate.Slot;

        // Store the tuple in slot, but make sure it's not freed.
        _ = NativeMethods.ExecStoreHeapTuple(remotetuple, slot, false);

        if (aestate.ResultRelInfo->ri_TrigDesc is not null &&
            aestate.ResultRelInfo->ri_TrigDesc->trig_insert_before_row)
        {
            if (!NativeMethods.ExecBRInsertTriggers(aestate.Estate, aestate.ResultRelInfo, slot))
            {
                _ = Worf.MemoryContextSwitchTo(oldctx);
                return;
            }
        }

        // Compute stored generated columns
        ComputeGenerated(aestate, CmdType.CMD_INSERT);

        // Check the constraints of the tuple
        if (relation->rd_att->constr is not null)
        {
            NativeMethods.ExecConstraints(aestate.ResultRelInfo, slot, aestate.Estate);
        }

        TupleTableSlot** buffered = mistate.BufferedTuples;
        if (buffered[mistate.NbufferedTuples] is null)
        {
            buffered[mistate.NbufferedTuples] = NativeMethods.table_slot_create(relation, null);
        }
        else
        {
            _ = NativeMethods.ExecClearTuple(buffered[mistate.NbufferedTuples]);
        }

        _ = NativeMethods.ExecCopySlot(buffered[mistate.NbufferedTuples], slot);
        mistate.NbufferedTuples++;
        _ = Worf.MemoryContextSwitchTo(oldctx);
    }

    /// <summary>
    /// Finish the MultiInsert.
    /// </summary>
    /// <param name="rel">The relation.</param>
    internal static void ApplyHeapMiFinish(WorfRelation rel)
    {
        _ = rel;
        ApplyMIState? mistate = s_pglmistate;
        if (mistate is null)
        {
            return;
        }

        ApplyHeapMiFlush();

        NativeMethods.FreeBulkInsertState(mistate.Bistate);

        FinishApplyExecState(mistate.Aestate);

        for (int i = 0; i < mistate.MaxbufferedTuples; i++)
        {
            if (mistate.BufferedTuples[i] is not null)
            {
                NativeMethods.ExecDropSingleTupleTableSlot(mistate.BufferedTuples[i]);
            }
        }

        NativeMethods.pfree(mistate.BufferedTuples);

        s_pglmistate = null;
    }

    private static List* UserTableUpdateOpenIndexes(ResultRelInfo* relinfo, EState* estate, TupleTableSlot* slot,
        bool update)
    {
        List* recheckIndexes = null;

        if (relinfo->ri_NumIndices > 0)
        {
            recheckIndexes = ExecInsertIndexTuples(relinfo, slot, estate, update);

            // FIXME: recheck the indexes
            if (recheckIndexes is not null)
            {
                List<string> idxnames = [];
                RelationData* targetRel = relinfo->ri_RelationDesc;

                string relname = Worf.FromServer((sbyte*)&targetRel->rd_rel->relname);
                string nspname = Worf.FromServer(NativeMethods.get_namespace_name(targetRel->rd_rel->relnamespace));

                for (int i = 0; i < recheckIndexes->length; i++)
                {
                    uint idxoid = recheckIndexes->elements[i].oid_value;
                    sbyte* idxname = NativeMethods.get_rel_name(idxoid);
                    if (idxname is null)
                    {
                        throw new PgException(PgSqlStates.InternalError, $"cache lookup failed for index oid {idxoid}");
                    }

                    idxnames.Add(Spi.QuoteIdentifier(Worf.FromServer(idxname)));
                }

                throw new PgException(PgSqlStates.FeatureNotSupported, "worf doesn't support deferrable indexes",
                    detail: $"relation {Spi.QuoteIdentifier(nspname)}.{Spi.QuoteIdentifier(relname)} has deferrable indexes: {string.Join(", ", idxnames)}");
            }

            NativeMethods.list_free(recheckIndexes);
        }

        return recheckIndexes;
    }

    private static bool PhysattInAttmap(WorfRelation rel, int attid)
    {
        for (int i = 0; i < rel.Natts; i++)
        {
            if (rel.Attmap[i] == attid)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Executes default values for columns for which we didn't get any data.
    /// </summary>
    /// <remarks>TODO: this needs caching, it's not exactly fast.</remarks>
    private static void FillMissingDefaults(WorfRelation rel, EState* estate, WorfTupleData tuple)
    {
        RelationData* relation = Relation(rel);
        TupleDescData* desc = relation->rd_att;
        int numPhysAttrs = desc->natts;
        int numDefaults = 0;

        ExprContext* econtext = GetPerTupleExprContext(estate);

        // We got all the data via replication, no need to evaluate anything.
        if (numPhysAttrs == rel.Natts)
        {
            return;
        }

        int[] defmap = new int[numPhysAttrs];
        ExprState*[] defexprs = new ExprState*[numPhysAttrs];

        for (int attnum = 0; attnum < numPhysAttrs; attnum++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, attnum);

            if (att->attisdropped)
            {
                continue;
            }

            if (PhysattInAttmap(rel, attnum))
            {
                continue;
            }

            // Skip generated columns - they are computed automatically by PostgreSQL
            if (att->attgenerated != 0)
            {
                continue;
            }

            Expr* defexpr = (Expr*)NativeMethods.build_column_default(relation, attnum + 1);

            if (defexpr is not null)
            {
                // Run the expression through planner
                defexpr = NativeMethods.expression_planner(defexpr);

                // Initialize executable expression in copycontext
                defexprs[numDefaults] = NativeMethods.ExecInitExpr(defexpr, null);
                defmap[numDefaults] = attnum;
                numDefaults++;
            }
        }

        for (int i = 0; i < numDefaults; i++)
        {
            tuple.Values[defmap[i]] = Executor.ExecEvalExpr(defexprs[i], econtext, out tuple.Nulls[defmap[i]]);
        }
    }

    private static ApplyExecState InitApplyExecState(WorfRelation rel)
    {
        RelationData* relation = Relation(rel);
        ApplyExecState aestate = new();

        // Initialize the executor state.
        aestate.Estate = Executor.CreateEstateForRelation(relation, true);

        aestate.ResultRelInfo = (ResultRelInfo*)NativeMethods.palloc0((ulong)sizeof(ResultRelInfo));
        aestate.ResultRelInfo->type = NodeTag.T_ResultRelInfo;
        NativeMethods.InitResultRelInfo(aestate.ResultRelInfo, relation, 1, null, 0);

#if ANKUS_PG13
        aestate.Estate->es_result_relations = aestate.ResultRelInfo;
        aestate.Estate->es_num_result_relations = 1;
        aestate.Estate->es_result_relation_info = aestate.ResultRelInfo;
#endif

        aestate.Slot = NativeMethods.ExecInitExtraTupleSlot(aestate.Estate, null,
            NativeGlobals.DangerousAddressOf_TTSOpsHeapTuple());
        NativeMethods.ExecSetSlotDescriptor(aestate.Slot, relation->rd_att);

        // PG18+ requires ecxt_scantuple for expression evaluation in FillMissingDefaults
        GetPerTupleExprContext(aestate.Estate)->ecxt_scantuple = aestate.Slot;

        aestate.Epqstate = (EPQState*)NativeMethods.palloc0((ulong)sizeof(EPQState));
        if (aestate.ResultRelInfo->ri_TrigDesc is not null)
        {
#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG15
            NativeMethods.EvalPlanQualInit(aestate.Epqstate, aestate.Estate, null, null, -1);
#else
            NativeMethods.EvalPlanQualInit(aestate.Epqstate, aestate.Estate, null, null, -1, null);
#endif
        }

        // Prepare to catch AFTER triggers.
        NativeMethods.AfterTriggerBeginQuery();

        return aestate;
    }

    private static void FinishApplyExecState(ApplyExecState aestate)
    {
        // Close indexes
        NativeMethods.ExecCloseIndices(aestate.ResultRelInfo);

        // Handle queued AFTER triggers.
        NativeMethods.AfterTriggerEndQuery(aestate.Estate);

        // Terminate EPQ execution if active.
        if (aestate.ResultRelInfo->ri_TrigDesc is not null)
        {
            NativeMethods.EvalPlanQualEnd(aestate.Epqstate);
        }

        // Cleanup tuple table.
        NativeMethods.ExecResetTupleTable(aestate.Estate->es_tupleTable, true);

        // Free the memory.
        NativeMethods.FreeExecutorState(aestate.Estate);
        NativeMethods.pfree(aestate.Epqstate);
    }

    /// <summary>
    /// MultiInsert initialization.
    /// </summary>
    private static void ApplyHeapMiStart(WorfRelation rel)
    {
        if (s_pglmistate is not null && s_pglmistate.Rel == rel)
        {
            return;
        }

        if (s_pglmistate is not null && s_pglmistate.Rel != rel)
        {
            ApplyHeapMiFinish(s_pglmistate.Rel);
        }

        MemoryContextData* oldctx = Worf.MemoryContextSwitchTo(NativeGlobals.TopTransactionContext);
        RelationData* relation = Relation(rel);
        bool volatileDefexprs = false;

        // Initialize the executor state.
        ApplyExecState aestate = InitApplyExecState(rel);
        _ = Worf.MemoryContextSwitchTo(NativeGlobals.TopTransactionContext);

        // Initialize new MultiInsert state.
        ApplyMIState mistate = new(rel, aestate);
        ResultRelInfo* resultRelInfo = aestate.ResultRelInfo;

        NativeMethods.ExecOpenIndices(resultRelInfo, false);

        // Check if table has any volatile default expressions.
        TupleDescData* desc = relation->rd_att;
        if (desc->natts != rel.Natts)
        {
            for (int attnum = 0; attnum < desc->natts; attnum++)
            {
                if (NativeMethods.TupleDescAttr(desc, attnum)->attisdropped)
                {
                    continue;
                }

                Expr* defexpr = (Expr*)NativeMethods.build_column_default(relation, attnum + 1);

                if (defexpr is not null)
                {
                    // Run the expression through planner
                    defexpr = NativeMethods.expression_planner(defexpr);
                    volatileDefexprs = NativeMethods.contain_volatile_functions_not_nextval((Node*)defexpr);

                    if (volatileDefexprs)
                    {
                        break;
                    }
                }
            }
        }

        // Decide if to buffer tuples based on the collected information about the table.
        mistate.MaxbufferedTuples = (resultRelInfo->ri_TrigDesc is not null &&
            (resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
             resultRelInfo->ri_TrigDesc->trig_insert_instead_row)) ||
            volatileDefexprs ? 1 : 1000;

        mistate.Cid = NativeMethods.GetCurrentCommandId(true);
        mistate.Bistate = NativeMethods.GetBulkInsertState();

        // Make the space for buffer.
        mistate.BufferedTuples = (TupleTableSlot**)NativeMethods.palloc0((ulong)(mistate.MaxbufferedTuples * sizeof(TupleTableSlot*)));
        mistate.NbufferedTuples = 0;

        s_pglmistate = mistate;

        _ = Worf.MemoryContextSwitchTo(oldctx);
    }

    /// <summary>
    /// Write the buffered tuples.
    /// </summary>
    private static void ApplyHeapMiFlush()
    {
        ApplyMIState? mistate = s_pglmistate;
        if (mistate is null || mistate.NbufferedTuples == 0)
        {
            return;
        }

        MemoryContextData* oldctx = Worf.MemoryContextSwitchTo(GetPerTupleMemoryContext(mistate.Aestate.Estate));
        NativeMethods.heap_multi_insert(Relation(mistate.Rel), mistate.BufferedTuples, mistate.NbufferedTuples,
            mistate.Cid, 0 /* hi_options */, mistate.Bistate);
        _ = Worf.MemoryContextSwitchTo(oldctx);

        ResultRelInfo* resultRelInfo = mistate.Aestate.ResultRelInfo;

        // If there are any indexes, update them for all the inserted tuples, and run AFTER ROW INSERT triggers.
        if (resultRelInfo->ri_NumIndices > 0)
        {
            for (int i = 0; i < mistate.NbufferedTuples; i++)
            {
                List* recheckIndexes = ExecInsertIndexTuples(resultRelInfo, mistate.BufferedTuples[i],
                    mistate.Aestate.Estate, false);
                NativeMethods.ExecARInsertTriggers(mistate.Aestate.Estate, resultRelInfo, mistate.BufferedTuples[i],
                    recheckIndexes, null);
                NativeMethods.list_free(recheckIndexes);
            }
        }

        // There's no indexes, but see if we need to run AFTER ROW INSERT triggers anyway.
        else if (resultRelInfo->ri_TrigDesc is not null && resultRelInfo->ri_TrigDesc->trig_insert_after_row)
        {
            for (int i = 0; i < mistate.NbufferedTuples; i++)
            {
                NativeMethods.ExecARInsertTriggers(mistate.Aestate.Estate, resultRelInfo, mistate.BufferedTuples[i],
                    null, null);
            }
        }

        mistate.NbufferedTuples = 0;
    }

    private static RelationData* Relation(WorfRelation rel) => rel.Rel;

    /// <summary>
    /// The heap tuple a heap tuple slot holds, as <c>TTS_TUP</c>.
    /// </summary>
    private static HeapTupleData* TtsTup(TupleTableSlot* slot) => ((HeapTupleTableSlot*)slot)->tuple;

    private static ExprContext* GetPerTupleExprContext(EState* estate) =>
        estate->es_per_tuple_exprcontext is not null
            ? estate->es_per_tuple_exprcontext
            : NativeMethods.MakePerTupleExprContext(estate);

    private static MemoryContextData* GetPerTupleMemoryContext(EState* estate) =>
        GetPerTupleExprContext(estate)->ecxt_per_tuple_memory;

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

    /// <summary>
    /// Compute stored generated columns if the relation has any. This must be called after storing the tuple in the
    /// slot but before the actual insert/update operation.
    /// </summary>
    private static void ComputeGenerated(ApplyExecState aestate, CmdType cmdtype)
    {
        TupleDescData* tupdesc = aestate.ResultRelInfo->ri_RelationDesc->rd_att;
        if (tupdesc->constr is not null && tupdesc->constr->has_generated_stored)
        {
#if ANKUS_PG13
            NativeMethods.ExecComputeStoredGenerated(aestate.Estate, aestate.Slot, cmdtype);
#else
            NativeMethods.ExecComputeStoredGenerated(aestate.ResultRelInfo, aestate.Estate, aestate.Slot, cmdtype);
#endif
        }
    }

    private static List* ExecInsertIndexTuples(ResultRelInfo* relinfo, TupleTableSlot* slot, EState* estate,
        bool update)
    {
#if ANKUS_PG13
        _ = relinfo;
        _ = update;
        return NativeMethods.ExecInsertIndexTuples(slot, estate, false, null, null);
#elif ANKUS_PG14 || ANKUS_PG15
        return NativeMethods.ExecInsertIndexTuples(relinfo, slot, estate, update, false, null, null);
#elif ANKUS_PG16 || ANKUS_PG17 || ANKUS_PG18
        return NativeMethods.ExecInsertIndexTuples(relinfo, slot, estate, update, false, null, null, false);
#else
        const uint EiitIsUpdate = 1 << 0;
        return NativeMethods.ExecInsertIndexTuples(relinfo, estate, update ? EiitIsUpdate : 0, slot, null, null);
#endif
    }

    /// <summary>
    /// Updates the local row, returning whether its indexes need new entries.
    /// </summary>
    private static bool SimpleTableTupleUpdate(RelationData* relation, ItemPointerData* otid, ApplyExecState aestate)
    {
#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG15
        bool updateIndexes;
        NativeMethods.simple_table_tuple_update(relation, otid, aestate.Slot, aestate.Estate->es_snapshot,
            &updateIndexes);
        return updateIndexes;
#else
        TU_UpdateIndexes updateIndexes;
        NativeMethods.simple_table_tuple_update(relation, otid, aestate.Slot, aestate.Estate->es_snapshot,
            &updateIndexes);
        return updateIndexes != TU_UpdateIndexes.TU_None;
#endif
    }

    private static bool ExecBRUpdateTriggers(ApplyExecState aestate, ItemPointerData* tupleid)
    {
#if ANKUS_PG13 || ANKUS_PG14
        return NativeMethods.ExecBRUpdateTriggers(aestate.Estate, aestate.Epqstate, aestate.ResultRelInfo, tupleid,
            null, aestate.Slot);
#elif ANKUS_PG15
        return NativeMethods.ExecBRUpdateTriggers(aestate.Estate, aestate.Epqstate, aestate.ResultRelInfo, tupleid,
            null, aestate.Slot, null);
#elif ANKUS_PG16 || ANKUS_PG17
        return NativeMethods.ExecBRUpdateTriggers(aestate.Estate, aestate.Epqstate, aestate.ResultRelInfo, tupleid,
            null, aestate.Slot, null, null);
#else
        return NativeMethods.ExecBRUpdateTriggers(aestate.Estate, aestate.Epqstate, aestate.ResultRelInfo, tupleid,
            null, aestate.Slot, null, null, false);
#endif
    }

    private static void ExecARUpdateTriggers(ApplyExecState aestate, ItemPointerData* tupleid, List* recheckIndexes)
    {
#if ANKUS_PG13 || ANKUS_PG14
        NativeMethods.ExecARUpdateTriggers(aestate.Estate, aestate.ResultRelInfo, tupleid, null, aestate.Slot,
            recheckIndexes, null);
#else
        NativeMethods.ExecARUpdateTriggers(aestate.Estate, aestate.ResultRelInfo, null, null, tupleid, null,
            aestate.Slot, recheckIndexes, null, false);
#endif
    }

    private static bool ExecBRDeleteTriggers(ApplyExecState aestate, ItemPointerData* tupleid)
    {
#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG15
        return NativeMethods.ExecBRDeleteTriggers(aestate.Estate, aestate.Epqstate, aestate.ResultRelInfo, tupleid,
            null, null);
#elif ANKUS_PG16 || ANKUS_PG17
        return NativeMethods.ExecBRDeleteTriggers(aestate.Estate, aestate.Epqstate, aestate.ResultRelInfo, tupleid,
            null, null, null, null);
#else
        return NativeMethods.ExecBRDeleteTriggers(aestate.Estate, aestate.Epqstate, aestate.ResultRelInfo, tupleid,
            null, null, null, null, false);
#endif
    }

    private static void ExecARDeleteTriggers(ApplyExecState aestate, ItemPointerData* tupleid)
    {
#if ANKUS_PG13 || ANKUS_PG14
        NativeMethods.ExecARDeleteTriggers(aestate.Estate, aestate.ResultRelInfo, tupleid, null, null);
#else
        NativeMethods.ExecARDeleteTriggers(aestate.Estate, aestate.ResultRelInfo, tupleid, null, null, false);
#endif
    }
}
