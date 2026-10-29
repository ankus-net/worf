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

    /// <summary>
    /// Try resolving the conflict resolution.
    /// </summary>
    /// <param name="localOrigin">The origin of the transaction that wrote the local row.</param>
    /// <param name="localTs">When that transaction committed.</param>
    /// <param name="localtuple">The local row.</param>
    /// <param name="remotetuple">The remote row.</param>
    /// <param name="resulttuple">The row to keep.</param>
    /// <param name="resolution">How the conflict was resolved.</param>
    /// <returns>Whether the remote tuple should be applied.</returns>
    internal static unsafe bool TryResolveConflict(ushort localOrigin, long localTs, HeapTupleData* localtuple,
        HeapTupleData* remotetuple, out HeapTupleData* resulttuple, out WorfConflictResolution resolution)
    {
        bool apply;

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
                apply = ConflictResolveByTimestamp(localOrigin, localTs, Worf.ReploriginSessionOriginTimestamp, true,
                    out resolution);
                break;

            case WorfResolveOption.FirstUpdateWins:
                apply = ConflictResolveByTimestamp(localOrigin, localTs, Worf.ReploriginSessionOriginTimestamp, false,
                    out resolution);
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
    /// Resolve conflict based on commit timestamp.
    /// </summary>
    private static bool ConflictResolveByTimestamp(ushort localOriginId, long localTs, long remoteTs, bool lastUpdateWins,
        out WorfConflictResolution resolution)
    {
        int cmp = remoteTs.CompareTo(localTs);

        // The logic bellow assumes last update wins, we invert the logic by inverting result of timestamp comparison
        // if first update wins was requested.
        if (!lastUpdateWins)
        {
            cmp = -cmp;
        }

        // The timestamps were equal, break the tie in a manner that is consistent across all nodes: the change from
        // the node with the higher id wins. A local row from the provider itself, or from a writer that isn't a
        // worf node, loses to the remote change.
        if (cmp == 0)
        {
            WorfSubscription sub = Apply.MySubscription!;
            uint? localNodeId = localOriginId == 0 ? sub.Target.Id : OriginNodeId(localOriginId);
            cmp = localNodeId is uint nodeId && nodeId != sub.Origin.Id ? sub.Origin.Id.CompareTo(nodeId) : 1;
        }

        if (cmp > 0)
        {
            // The remote row wins, update the local one.
            resolution = WorfConflictResolution.ApplyRemote;
            return true;
        }
        else
        {
            // The local row wins, retain it
            resolution = WorfConflictResolution.KeepLocal;
            return false;
        }
    }

    /// <summary>
    /// Gets the node whose changes a replication origin applies: the provider of the subscription whose apply or
    /// table sync worker uses it.
    /// </summary>
    /// <param name="originId">The replication origin.</param>
    /// <returns>The provider node's id, or null when no subscription uses the origin.</returns>
    private static uint? OriginNodeId(ushort originId)
    {
        SpiResult result = Worf.SelectCatalog("""
            SELECT s.sub_origin
            FROM pg_catalog.pg_replication_origin o
                JOIN worf.subscription s ON o.roname = s.sub_slot_name::text
                    OR (length(o.roname) = length(s.sub_slot_name::text) + 9 AND starts_with(o.roname, s.sub_slot_name || '_'))
            WHERE o.roident = $1
            ORDER BY o.roname = s.sub_slot_name::text DESC
            LIMIT 1
            """, SpiParameter.Create((uint)originId));
        return result.Count == 0 ? null : result[0].Get<uint>(0);
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
