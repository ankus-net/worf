using Ankus.Postgres;
using Path = System.IO.Path;

namespace Worf;

/// <summary>
/// Logical Replication output plugin.
/// </summary>
internal static unsafe partial class OutputPlugin
{
    private const int RelmetacacheInitialSize = 128;

    private static readonly Dictionary<nint, WorfOutputData> s_outputData = [];

    private static bool s_startupMessageSent;

    private static Dictionary<uint, RelMetaCacheEntry>? s_relMetaCache;

    private static int s_invalidRelMetaCacheCnt;

    [PgNativeCallback(nameof(PgDecodeStartup))]
    private static partial OutputPluginCallbacks_startup_cbCallback StartupCallback { get; }

    [PgNativeCallback(nameof(PgDecodeBeginTxn))]
    private static partial OutputPluginCallbacks_begin_cbCallback BeginCallback { get; }

    [PgNativeCallback(nameof(PgDecodeChange))]
    private static partial OutputPluginCallbacks_change_cbCallback ChangeCallback { get; }

    [PgNativeCallback(nameof(PgDecodeCommitTxn))]
    private static partial OutputPluginCallbacks_commit_cbCallback CommitCallback { get; }

    [PgNativeCallback(nameof(PgDecodeOriginFilter))]
    private static partial OutputPluginCallbacks_filter_by_origin_cbCallback OriginFilterCallback { get; }

    [PgNativeCallback(nameof(PgDecodeShutdown))]
    private static partial OutputPluginCallbacks_shutdown_cbCallback ShutdownCallback { get; }

    [PgNativeCallback(nameof(RelmetacacheInvalidationCb))]
#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static partial RelcacheCallbackFunction RelmetacacheInvalidationCallback { get; }
#else
    private static partial RelSyncCallbackFunction RelmetacacheInvalidationCallback { get; }
#endif

    /// <summary>
    /// Specify output plugin callbacks.
    /// </summary>
    /// <param name="cb">PostgreSQL's callback table.</param>
    [PgOutputPlugin]
    internal static void PgOutputPluginInit(OutputPluginCallbacks* cb)
    {
        cb->startup_cb = StartupCallback;
        cb->begin_cb = BeginCallback;
        cb->change_cb = ChangeCallback;
        cb->commit_cb = CommitCallback;
        cb->filter_by_origin_cb = OriginFilterCallback;
        cb->shutdown_cb = ShutdownCallback;
    }

    private static bool CheckBinaryCompatibility(WorfOutputData data)
    {
        if (data.ClientBinaryBasetypesMajorVersion != Worf.ServerVersionNum / 100)
        {
            return false;
        }

        if (data.ClientBinaryBigendianSet && data.ClientBinaryBigendian != OutputConfig.ServerBigendian)
        {
            PgLog.Debug1("Binary mode rejected: Server and client endian mismatch");
            return false;
        }

        if (data.ClientBinarySizeofdatum != 0 && data.ClientBinarySizeofdatum != Worf.SizeofDatum)
        {
            PgLog.Debug1("Binary mode rejected: Server and client sizeof(Datum) mismatch");
            return false;
        }

        if (data.ClientBinarySizeofint != 0 && data.ClientBinarySizeofint != Worf.SizeofInt)
        {
            PgLog.Debug1("Binary mode rejected: Server and client sizeof(int) mismatch");
            return false;
        }

        if (data.ClientBinarySizeoflong != 0 && data.ClientBinarySizeoflong != Worf.SizeofLong)
        {
            PgLog.Debug1("Binary mode rejected: Server and client sizeof(long) mismatch");
            return false;
        }

        if (data.ClientBinaryFloat4byvalSet && data.ClientBinaryFloat4byval != OutputConfig.ServerFloat4Byval)
        {
            PgLog.Debug1("Binary mode rejected: Server and client float4byval mismatch");
            return false;
        }

        if (data.ClientBinaryFloat8byvalSet && data.ClientBinaryFloat8byval != OutputConfig.ServerFloat8Byval)
        {
            PgLog.Debug1("Binary mode rejected: Server and client float8byval mismatch");
            return false;
        }

        if (data.ClientBinaryIntdatetimesSet && data.ClientBinaryIntdatetimes != OutputConfig.ServerIntegerDatetimes)
        {
            PgLog.Debug1("Binary mode rejected: Server and client integer datetimes mismatch");
            return false;
        }

        return true;
    }

    /// <summary>
    /// Initialize this plugin.
    /// </summary>
    private static void PgDecodeStartup(LogicalDecodingContext* ctx, OutputPluginOptions* opt, bool isInit)
    {
        WorfOutputData data = new()
        {
            AllowInternalBasetypes = false,
            AllowBinaryBasetypes = false,
        };

        s_outputData[(nint)ctx] = data;

        // This is replication start and not slot initialization.
        //
        // Parse and validate options passed by the client.
        if (!isInit)
        {
            bool startedTx = false;

            // There's a potential corruption bug in PostgreSQL 10.1, 9.6.6, 9.5.10 and 9.4.15 that can cause reorder
            // buffers to accumulate duplicated transactions.
            //
            // We can defend against this by doing our own cleanup of any serialized txns in the reorder buffer on
            // startup.
            PglReorderBufferCleanSerializedTXNs(Worf.FromServer((sbyte*)&NativeGlobals.MyReplicationSlot->data.name));

            if (!NativeMethods.IsTransactionState())
            {
                NativeMethods.StartTransactionCommand();
                startedTx = true;
            }

            data.LocalNodeId = LocalNodeId();

            // Ideally we'd send the startup message immediately. That way it'd arrive before any error we emit if we
            // see incompatible options sent by the client here.
            //
            // The decoding plugin API doesn't let us write to the stream from here, though, so we have to delay the
            // startup message until the first change processed on the stream, in a begin callback.
            s_startupMessageSent = false;

            // Now parse the rest of the params and ERROR if we see any we don't recognise
            int paramsFormat = OutputConfig.ProcessParameters(ReadOptions(ctx->output_plugin_options), data);

            if (paramsFormat != 1)
            {
                throw new PgException(PgSqlStates.FeatureNotSupported,
                    $"client sent startup parameters in format {paramsFormat} but we only support format 1");
            }

            if (data.ClientMinProtoVersion > OutputProto.ProtoVersionNum)
            {
                throw new PgException(PgSqlStates.FeatureNotSupported,
                    $"client sent min_proto_version={data.ClientMinProtoVersion} but we only support protocol {OutputProto.ProtoVersionNum} or lower");
            }

            if (data.ClientMaxProtoVersion < OutputProto.ProtoMinVersionNum)
            {
                throw new PgException(PgSqlStates.FeatureNotSupported,
                    $"client sent max_proto_version={data.ClientMaxProtoVersion} but we only support protocol {OutputProto.ProtoMinVersionNum} or higher");
            }

            // Set correct protocol format.
            //
            // This is the output plugin protocol format, this is different from the individual fields binary vs
            // textual format.
            if (data.ClientProtocolFormat == "json")
            {
                data.Api = OutputProto.InitApi(WorfProtoType.Json);
                opt->output_type = OutputPluginOutputType.OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
            }
            else if (data.ClientProtocolFormat is null or "native")
            {
                data.Api = OutputProto.InitApi(WorfProtoType.Native);
                opt->output_type = OutputPluginOutputType.OUTPUT_PLUGIN_BINARY_OUTPUT;

                if (data.ClientNoTxinfo)
                {
                    PgLog.Warning("no_txinfo option ignored for protocols other than json");
                    data.ClientNoTxinfo = false;
                }
            }
            else
            {
                throw new PgException(PgSqlStates.FeatureNotSupported,
                    $"client requested protocol {data.ClientProtocolFormat} but only \"json\" or \"native\" are supported");
            }

            // check for encoding match if specific encoding demanded by client
            if (!string.IsNullOrEmpty(data.ClientExpectedEncoding))
            {
                int wantedEncoding = OutputConfig.CharToEncoding(data.ClientExpectedEncoding);

                if (wantedEncoding == -1)
                {
                    throw new PgException(PgSqlStates.InvalidParameterValue,
                        $"unrecognised encoding name {data.ClientExpectedEncoding} passed to expected_encoding");
                }

                if (opt->output_type == OutputPluginOutputType.OUTPUT_PLUGIN_TEXTUAL_OUTPUT)
                {
                    // datum encoding must match assigned client_encoding in text proto, since everything is subject
                    // to client_encoding conversion.
                    if (wantedEncoding != NativeMethods.pg_get_client_encoding())
                    {
                        throw new PgException(PgSqlStates.InvalidParameterValue,
                            "expected_encoding must be unset or match client_encoding in text protocols");
                    }
                }
                else
                {
                    // currently in the binary protocol we can only emit encoded datums in the server encoding. There's
                    // no support for encoding conversion.
                    if (wantedEncoding != NativeMethods.GetDatabaseEncoding())
                    {
                        throw new PgException(PgSqlStates.FeatureNotSupported,
                            "encoding conversion for binary datum not supported yet",
                            detail: $"expected_encoding {data.ClientExpectedEncoding} must be unset or match server_encoding {Worf.DatabaseEncodingName}");
                    }
                }

                data.FieldDatumEncoding = wantedEncoding;
            }

            // It's obviously not possible to send binary representation of data unless we use the binary output.
            if (opt->output_type == OutputPluginOutputType.OUTPUT_PLUGIN_BINARY_OUTPUT && data.ClientWantInternalBasetypes)
            {
                data.AllowInternalBasetypes = CheckBinaryCompatibility(data);
            }

            if (opt->output_type == OutputPluginOutputType.OUTPUT_PLUGIN_BINARY_OUTPUT &&
                data.ClientWantBinaryBasetypes &&
                data.ClientBinaryBasetypesMajorVersion == Worf.ServerVersionNum / 100)
            {
                data.AllowBinaryBasetypes = true;
            }

            // There's currently no knob for clients to use to suppress this info and it's sent if it's supported and
            // available.
            data.ForwardChangesetOrigins = true;

            if (startedTx)
            {
                NativeMethods.CommitTransactionCommand();
            }

            RelmetacacheInit();
        }
    }

    /// <summary>
    /// BEGIN callback.
    /// </summary>
    private static void PgDecodeBeginTxn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn)
    {
        WorfOutputData data = Data(ctx);
        bool sendReplicationOrigin = data.ForwardChangesetOrigins;

        if (!s_startupMessageSent)
        {
            SendStartupMessage(ctx, data, false /* can't be last message */);
        }

        // If the record didn't originate locally, send origin info
        sendReplicationOrigin &= txn->origin_id != 0;

        WorfProtoApi api = data.Api!;
        NativeMethods.OutputPluginPrepareWrite(ctx, !sendReplicationOrigin);
        api.WriteBegin(ctx->@out, data, txn);

        if (sendReplicationOrigin)
        {
            sbyte* origin;

            // Message boundary
            NativeMethods.OutputPluginWrite(ctx, false);
            NativeMethods.OutputPluginPrepareWrite(ctx, true);

            // XXX: which behaviour we want here?
            //
            // Alternatives:
            //  - don't send origin message if origin name not found (that's what we do now)
            //  - throw error - that will break replication, not good
            //  - send some special "unknown" origin
            if (api.WriteOrigin is not null && NativeMethods.replorigin_by_oid(txn->origin_id, true, &origin))
            {
                api.WriteOrigin(ctx->@out, origin, txn->origin_lsn);
            }
        }

        NativeMethods.OutputPluginWrite(ctx, true);
    }

    /// <summary>
    /// COMMIT callback.
    /// </summary>
    private static void PgDecodeCommitTxn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn, ulong commitLsn)
    {
        WorfOutputData data = Data(ctx);

        NativeMethods.OutputPluginPrepareWrite(ctx, true);
        data.Api!.WriteCommit(ctx->@out, data, txn, commitLsn);
        NativeMethods.OutputPluginWrite(ctx, true);

        // Now is a good time to get rid of invalidated relation metadata entries since nothing will be referencing
        // them at the moment.
        RelmetacachePrune();
    }

    private static bool WorfChangeFilter(WorfOutputData data, RelationData* relation, ReorderBufferChange* change,
        out HashSet<int>? attList)
    {
        attList = null;

        if (data.ReplicateOnlyTable is (string schemaname, string relname))
        {
            // Special case - we are catching up just one table.
            // TODO: performance
            uint nspoid;
            fixed (byte* cschemaname = Worf.ToServerCString(schemaname))
            {
                nspoid = NativeMethods.get_namespace_oid((sbyte*)cschemaname, true);
            }

            return Worf.FromServer((sbyte*)&relation->rd_rel->relname) == relname &&
                relation->rd_rel->relnamespace == nspoid;
        }
        else if (relation->rd_id == Queue.GetQueueTableOid())
        {
            // Special case - queue table
            if (change->action == ReorderBufferChangeType.REORDER_BUFFER_CHANGE_INSERT)
            {
                HeapTupleData* tup = NewTuple(change);

                NativeMethods.LockRelation(relation, (int)PgLockMode.AccessShare);
                QueuedMessage q = Queue.QueuedMessageFromTuple(tup);
                NativeMethods.UnlockRelation(relation, (int)PgLockMode.AccessShare);

                // No replication set means global message, those are always replicated, but excluding TRUNCATE.
                if (q.ReplicationSets is null && q.MessageType != Queue.QueueCommandTypeTruncate)
                {
                    return true;
                }

                foreach (string queueSet in q.ReplicationSets ?? [])
                {
                    foreach (WorfRepSet rs in data.ReplicationSets)
                    {
                        // TODO: this is somewhat ugly.
                        if (queueSet == rs.Name &&
                            (q.MessageType != Queue.QueueCommandTypeTruncate || rs.ReplicateTruncate))
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }
        else if (relation->rd_id == ReplicationSet.GetReplicationSetRelOid())
        {
            // Special case - replication set table.
            //
            // We can use this to update our cached replication set info, without having to deal with cache
            // invalidation callbacks.
            HeapTupleData* tup;

            if (change->action == ReorderBufferChangeType.REORDER_BUFFER_CHANGE_UPDATE)
            {
                tup = NewTuple(change);
            }
            else if (change->action == ReorderBufferChangeType.REORDER_BUFFER_CHANGE_DELETE)
            {
                tup = OldTuple(change);
            }
            else
            {
                return false;
            }

            WorfRepSet replicatedSet = ReplicationSet.ReplicationSetFromTuple(tup, relation->rd_att);
            foreach (WorfRepSet rs in data.ReplicationSets)
            {
                // Check if the changed repset is used by us.
                if (rs.Id == replicatedSet.Id)
                {
                    // In case this was delete, somebody deleted one of our rep sets, bail here and let reconnect logic
                    // handle any potential issues.
                    if (change->action == ReorderBufferChangeType.REORDER_BUFFER_CHANGE_DELETE)
                    {
                        throw new PgException(PgSqlStates.InternalError,
                            $"replication set \"{rs.Name}\" used by this connection was deleted, existing");
                    }

                    // This was update of our repset, update the cache.
                    rs.ReplicateInsert = replicatedSet.ReplicateInsert;
                    rs.ReplicateUpdate = replicatedSet.ReplicateUpdate;
                    rs.ReplicateDelete = replicatedSet.ReplicateDelete;
                    rs.ReplicateTruncate = replicatedSet.ReplicateTruncate;

                    return false;
                }
            }

            return false;
        }

        // Normal case - use replication set membership.
        WorfTableRepInfo tblinfo = ReplicationSet.GetTableReplicationInfo(data.LocalNodeId, relation, data.ReplicationSets);

        // First try filter out by change type.
        switch (change->action)
        {
            case ReorderBufferChangeType.REORDER_BUFFER_CHANGE_INSERT:
                if (!tblinfo.ReplicateInsert)
                {
                    return false;
                }

                break;

            case ReorderBufferChangeType.REORDER_BUFFER_CHANGE_UPDATE:
                if (!tblinfo.ReplicateUpdate)
                {
                    return false;
                }

                break;

            case ReorderBufferChangeType.REORDER_BUFFER_CHANGE_DELETE:
                if (!tblinfo.ReplicateDelete)
                {
                    return false;
                }

                break;

            default:
                throw new PgException(PgSqlStates.InternalError, $"Unhandled reorder buffer change type {(int)change->action}");
        }

        // Proccess row filters.
        // XXX: we could probably cache some of the executor stuff.
        if (tblinfo.RowFilter.Count > 0)
        {
            TupleDescData* tupdesc = relation->rd_att;
            HeapTupleData* oldtup = OldTuple(change);
            HeapTupleData* newtup = NewTuple(change);

            // Skip empty changes.
            if (newtup is null && oldtup is null)
            {
                PgLog.Debug1("worf output got empty change");
                return false;
            }

            NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());
            try
            {
                EState* estate = Executor.CreateEstateForRelation(relation, false);
                ExprContext* econtext = Executor.PreparePerTupleEcontext(estate, tupdesc);

                _ = NativeMethods.ExecStoreHeapTuple(newtup is not null ? newtup : oldtup, econtext->ecxt_scantuple, false);

                // Next try the row_filters if there are any.
                foreach (nint rowFilter in tblinfo.RowFilter)
                {
                    ExprState* exprstate = Executor.PrepareRowFilter((Node*)rowFilter);
                    ulong res = Executor.ExecEvalExpr(exprstate, econtext, out bool isnull);

                    // NULL is same as false for our use.
                    if (isnull)
                    {
                        return false;
                    }

                    if ((res & 0xFF) == 0)
                    {
                        return false;
                    }
                }

                NativeMethods.ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
                NativeMethods.FreeExecutorState(estate);
            }
            finally
            {
                NativeMethods.PopActiveSnapshot();
            }
        }

        // Make sure caller is aware of any attribute filter.
        attList = tblinfo.AttList;

        return true;
    }

    private static void PgDecodeChange(LogicalDecodingContext* ctx, ReorderBufferTXN* txn, RelationData* relation,
        ReorderBufferChange* change)
    {
        _ = txn;
        WorfOutputData data = Data(ctx);

        // Avoid leaking memory by using and resetting our own context
        PgMemoryContext.RunTransient("worf output msg context", _ =>
        {
            // First check the table filter
            if (!WorfChangeFilter(data, relation, change, out HashSet<int>? attList))
            {
                return;
            }

            // If the protocol wants to write relation information and the client isn't known to have metadata cached
            // for this relation already, send relation metadata.
            //
            // TODO: track hit/miss stats
            if (data.Api!.WriteRel is not null)
            {
                RelMetaCacheEntry cachedRelmeta = RelmetacacheGetRelation(relation);

                if (!cachedRelmeta.IsCached)
                {
                    NativeMethods.OutputPluginPrepareWrite(ctx, false);
                    data.Api.WriteRel(ctx->@out, data, relation, attList);
                    NativeMethods.OutputPluginWrite(ctx, false);
                    cachedRelmeta.IsCached = true;
                }
            }

            // Send the data
            switch (change->action)
            {
                case ReorderBufferChangeType.REORDER_BUFFER_CHANGE_INSERT:
                    NativeMethods.OutputPluginPrepareWrite(ctx, true);
                    data.Api.WriteInsert(ctx->@out, data, relation, NewTuple(change), attList);
                    NativeMethods.OutputPluginWrite(ctx, true);
                    break;

                case ReorderBufferChangeType.REORDER_BUFFER_CHANGE_UPDATE:
                {
                    HeapTupleData* oldtuple = OldTuple(change);

                    NativeMethods.OutputPluginPrepareWrite(ctx, true);
                    data.Api.WriteUpdate(ctx->@out, data, relation, oldtuple, NewTuple(change), attList);
                    NativeMethods.OutputPluginWrite(ctx, true);
                    break;
                }

                case ReorderBufferChangeType.REORDER_BUFFER_CHANGE_DELETE:
                    if (OldTuple(change) is not null)
                    {
                        NativeMethods.OutputPluginPrepareWrite(ctx, true);
                        data.Api.WriteDelete(ctx->@out, data, relation, OldTuple(change), attList);
                        NativeMethods.OutputPluginWrite(ctx, true);
                    }
                    else
                    {
                        PgLog.Debug1("didn't send DELETE change because of missing oldtuple");
                    }

                    break;

                default:
                    break;
            }
        });
    }

    /// <summary>
    /// Decide if the whole transaction with specific origin should be filtered out.
    /// </summary>
    private static bool PgDecodeOriginFilter(LogicalDecodingContext* ctx, ushort originId)
    {
        WorfOutputData data = Data(ctx);
        bool ret;

        if (originId == 0)
        {
            // Never filter out locally originated tx's
            ret = false;
        }
        else
        {
            // Otherwise, ignore the origin passed in txnfilter_args->origin_id, and just forward all or nothing
            // based on the configuration option 'forward_origins'.
            ret = data.ForwardOrigins.Count == 0;
        }

        return ret;
    }

    private static void SendStartupMessage(LogicalDecodingContext* ctx, WorfOutputData data, bool lastMessage)
    {
        List<(string Key, string Value)> msg = OutputConfig.PrepareStartupMessage(data);

        NativeMethods.OutputPluginPrepareWrite(ctx, lastMessage);
        data.Api!.WriteStartupMessage(ctx->@out, msg);
        NativeMethods.OutputPluginWrite(ctx, lastMessage);

        s_startupMessageSent = true;
    }

    /// <summary>
    /// Shutdown callback.
    /// </summary>
    private static void PgDecodeShutdown(LogicalDecodingContext* ctx)
    {
        RelmetacacheFlush();

        _ = s_outputData.Remove((nint)ctx);
    }

    /// <summary>
    /// Relation metadata invalidation, for when a relcache invalidation means that we need to resend table metadata
    /// to the client.
    /// </summary>
    private static void RelmetacacheInvalidationCb(ulong arg, uint relid)
    {
        _ = arg;

        // Nobody keeps pointers to entries in this hash table around outside logical decoding callback calls - but
        // invalidation events can come in *during* a callback if we access the relcache in the callback. Because of
        // that we must mark the cache entry as invalid but not remove it from the hash while it could still be
        // referenced, then prune it at a later safe point.
        //
        // Getting invalidations for relations that aren't in the table is entirely normal, since there's no way to
        // unregister for an invalidation event. So we don't care if it's found or not.
        if (s_relMetaCache is not null && s_relMetaCache.TryGetValue(relid, out RelMetaCacheEntry? hentry))
        {
            hentry.IsValid = false;
            s_invalidRelMetaCacheCnt++;
        }
    }

    /// <summary>
    /// Initialize the relation metadata cache for a decoding session.
    /// </summary>
    private static void RelmetacacheInit()
    {
        s_invalidRelMetaCacheCnt = 0;

        if (s_relMetaCache is null)
        {
            // Make a new hash table for the cache
            s_relMetaCache = new Dictionary<uint, RelMetaCacheEntry>(RelmetacacheInitialSize);

            NativeMethods.CacheRegisterRelcacheCallback(RelmetacacheInvalidationCallback, 0);
        }
    }

    /// <summary>
    /// Look up an entry, creating it if not found. Newly created entries are returned as is_cached=false.
    /// </summary>
    private static RelMetaCacheEntry RelmetacacheGetRelation(RelationData* rel)
    {
        // Find cached function info, creating if not found
        if (!s_relMetaCache!.TryGetValue(rel->rd_id, out RelMetaCacheEntry? hentry))
        {
            hentry = new RelMetaCacheEntry { Relid = rel->rd_id, IsValid = false };
            s_relMetaCache[rel->rd_id] = hentry;
        }

        // If not found or not valid, it can't be cached.
        if (!hentry.IsValid)
        {
            hentry.IsCached = false;

            // Only used for lazy purging of invalidations
            hentry.IsValid = true;
        }

        return hentry;
    }

    /// <summary>
    /// Flush the relation metadata cache at the end of a decoding session.
    /// </summary>
    private static void RelmetacacheFlush() => s_relMetaCache?.Clear();

    /// <summary>
    /// Prune !is_valid entries from the relation metadata cache.
    /// </summary>
    private static void RelmetacachePrune()
    {
        // Since the pruning can be expensive, do it only if ig we invalidated at least half of initial cache size.
        if (s_invalidRelMetaCacheCnt < RelmetacacheInitialSize / 2)
        {
            return;
        }

        Dictionary<uint, RelMetaCacheEntry> relMetaCache = s_relMetaCache!;
        foreach (uint relid in relMetaCache.Where(static entry => !entry.Value.IsValid).Select(static entry => entry.Key).ToList())
        {
            _ = relMetaCache.Remove(relid);
        }

        s_invalidRelMetaCacheCnt = 0;
    }

    /// <summary>
    /// Clone of ReorderBufferCleanSerializedTXNs.
    /// </summary>
    private static void PglReorderBufferCleanSerializedTXNs(string slotname)
    {
        string path = $"pg_replslot/{slotname}";

        // we're only handling directories here, skip if it's not ours
        if (!Directory.Exists(path))
        {
            return;
        }

        foreach (string spill in Directory.EnumerateFileSystemEntries(path))
        {
            // only look at names that can be ours
            string name = Path.GetFileName(spill);
            if (name.StartsWith("xid", StringComparison.Ordinal))
            {
                try
                {
                    File.Delete($"pg_replslot/{slotname}/{name}");
                }
                catch (IOException error)
                {
                    throw new PgException(PgSqlStates.IoError,
                        $"could not remove file \"pg_replslot/{slotname}/{name}\" during removal of pg_replslot/{slotname}/*.xid: {error.Message}");
                }
            }
        }
    }

    private static List<(string Name, string? Value)> ReadOptions(List* options)
    {
        List<(string Name, string? Value)> result = [];
        for (int i = 0; options is not null && i < options->length; i++)
        {
            DefElem* elem = (DefElem*)options->elements[i].ptr_value;
            string? value = null;
            if (elem->arg is not null)
            {
#if ANKUS_PG13 || ANKUS_PG14
                sbyte* str = ((Value*)elem->arg)->val.str;
#else
                sbyte* str = ((Ankus.Postgres.String*)elem->arg)->sval;
#endif
                value = str is null ? null : Worf.FromServer(str);
            }

            result.Add((Worf.FromServer(elem->defname), value));
        }

        return result;
    }

    private static WorfOutputData Data(LogicalDecodingContext* ctx) => s_outputData[(nint)ctx];

    /// <summary>
    /// Finds the local node, as <c>get_local_node</c> does for the output plugin.
    /// </summary>
    private static uint LocalNodeId()
    {
        RangeVar* rv = NativeMethods.makeRangeVar(Worf.PStrDup(Worf.ExtensionName), Worf.PStrDup("local_node"), -1);
        uint relid = NativeMethods.RangeVarGetRelidExtended(rv, (int)PgLockMode.AccessShare, 1 /* RVR_MISSING_OK */, default, null);
        if (relid == 0)
        {
            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "local worf node not found");
        }

        RelationData* rel = NativeMethods.table_open(relid, (int)PgLockMode.None);
        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 0, null);
        HeapTupleData* tuple = NativeMethods.systable_getnext(scan);
        if (tuple is null)
        {
            throw new PgException(PgSqlStates.ObjectNotInPrerequisiteState, "local worf node not found");
        }

        bool isnull;
        uint nodeid = (uint)NativeMethods.heap_getattr(tuple, 1, rel->rd_att, &isnull);

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, (int)PgLockMode.None);

        return nodeid;
    }

    /// <summary>
    /// Finds the named replication sets of a node, as <c>get_replication_sets</c> does for the output plugin.
    /// </summary>
    internal static List<WorfRepSet> ReplicationSetsByName(uint nodeid, List<string> replicationSetNames)
    {
        List<WorfRepSet> nodeSets = [];
        RangeVar* rv = NativeMethods.makeRangeVar(Worf.PStrDup(Worf.ExtensionName), Worf.PStrDup("replication_set"), -1);
        RelationData* rel = NativeMethods.table_openrv(rv, (int)PgLockMode.RowExclusive);
        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, true, null, 0, null);

        HeapTupleData* tuple;
        while ((tuple = NativeMethods.systable_getnext(scan)) is not null)
        {
            WorfRepSet repset = ReplicationSet.ReplicationSetFromTuple(tuple, rel->rd_att);
            if (repset.NodeId == nodeid)
            {
                nodeSets.Add(repset);
            }
        }

        NativeMethods.systable_endscan(scan);
        NativeMethods.table_close(rel, (int)PgLockMode.RowExclusive);

        List<WorfRepSet> replicationSets = [];
        foreach (string name in replicationSetNames)
        {
            WorfRepSet repset = nodeSets.Find(set => set.Name == name)
                ?? throw new PgException(PgSqlStates.InvalidParameterValue, $"replication set {name} not found");
            replicationSets.Add(repset);
        }

        return replicationSets;
    }

#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG15 || ANKUS_PG16
    private static HeapTupleData* NewTuple(ReorderBufferChange* change)
        => change->data.tp.newtuple is null ? null : &change->data.tp.newtuple->tuple;

    private static HeapTupleData* OldTuple(ReorderBufferChange* change)
        => change->data.tp.oldtuple is null ? null : &change->data.tp.oldtuple->tuple;
#else
    private static HeapTupleData* NewTuple(ReorderBufferChange* change) => change->data.tp.newtuple;

    private static HeapTupleData* OldTuple(ReorderBufferChange* change) => change->data.tp.oldtuple;
#endif
}
