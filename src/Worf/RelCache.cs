using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Caching relation specific information.
/// </summary>
internal static partial class RelCache
{
    private static Dictionary<uint, WorfRelation>? s_relationHash;

    /// <summary>
    /// The entries that map to each local relation. pglogical scans every entry on each invalidation instead, which
    /// makes a transaction that changes many replicated tables take quadratic time.
    /// </summary>
    private static readonly Dictionary<uint, List<WorfRelation>> s_relationsByReloid = [];

    [PgNativeCallback(nameof(RelcacheInvalidateCallback))]
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static partial RelcacheCallbackFunction InvalidateCallback { get; }
#else
    private static partial RelSyncCallbackFunction InvalidateCallback { get; }
#endif

    /// <summary>
    /// Opens the local relation a remote relation maps to.
    /// </summary>
    /// <param name="remoteid">The remote relation's id.</param>
    /// <param name="lockmode">The lock to take.</param>
    /// <returns>The relation.</returns>
    internal static unsafe WorfRelation RelationOpen(uint remoteid, PgLockMode lockmode)
    {
        Dictionary<uint, WorfRelation> hash = s_relationHash ??= RelcacheInit();

        // Search for existing entry.
        if (!hash.TryGetValue(remoteid, out WorfRelation? entry))
        {
            throw new PgException(PgSqlStates.InternalError, $"cache lookup failed for remote relation {remoteid}");
        }

        // Need to update the local cache?
        if (entry.Reloid == 0)
        {
            RangeVar* rv = NativeMethods.makeRangeVar(Worf.PStrDup(entry.Nspname), Worf.PStrDup(entry.Relname), -1);
            entry.Rel = NativeMethods.table_openrv(rv, (int)lockmode);

            TupleDescData* desc = entry.Rel->rd_att;
            for (int i = 0; i < entry.Natts; i++)
            {
                entry.Attmap[i] = TupdescGetAttByName(desc, entry.Attnames[i]);

                // A column dropped here is ignored in rows from a node that still has it.
                if (entry.Attmap[i] < 0 && !ColumnDropped(entry.Rel->rd_id, entry.Attnames[i]))
                {
                    throw new PgException(PgSqlStates.InternalError, $"unknown column name {entry.Attnames[i]}");
                }

                entry.Conversions[i] = entry.Attmap[i] >= 0 && entry.RemoteTypes[i] is WorfRemoteType remote
                    ? ColumnConversion(entry, remote, NativeMethods.TupleDescAttr(desc, entry.Attmap[i]))
                    : null;
            }

            entry.Reloid = entry.Rel->rd_id;
            if (!s_relationsByReloid.TryGetValue(entry.Reloid, out List<WorfRelation>? entries))
            {
                entries = [];
                s_relationsByReloid.Add(entry.Reloid, entries);
            }

            entries.Add(entry);

            // Cache trigger info.
            entry.HasTriggers = false;
            TriggerDesc* trigdesc = entry.Rel->trigdesc;
            if (trigdesc is not null)
            {
                for (int i = 0; i < trigdesc->numtriggers; i++)
                {
                    Trigger* trigger = &trigdesc->triggers[i];

                    // We only fire replica triggers on rows
                    if (!(trigger->tgenabled == TriggerFiresOnOrigin || trigger->tgenabled == TriggerDisabled) &&
                        (trigger->tgtype & TriggerTypeRow) != 0)
                    {
                        entry.HasTriggers = true;
                        break;
                    }
                }
            }
        }
        else if (entry.Rel is null)
        {
            entry.Rel = NativeMethods.table_open(entry.Reloid, (int)lockmode);
        }

        return entry;
    }

    /// <summary>
    /// Records a remote relation's description.
    /// </summary>
    /// <param name="remoteid">The remote relation's id.</param>
    /// <param name="schemaname">The relation's schema.</param>
    /// <param name="relname">The relation's name.</param>
    /// <param name="attnames">The remote columns' names.</param>
    /// <param name="types">The remote columns' types, or null when the provider didn't say.</param>
    internal static void RelationCacheUpdate(uint remoteid, string schemaname, string relname, string[] attnames,
        WorfRemoteType?[]? types = null)
    {
        Dictionary<uint, WorfRelation> hash = s_relationHash ??= RelcacheInit();

        if (hash.TryGetValue(remoteid, out WorfRelation? old) && s_relationsByReloid.TryGetValue(old.Reloid, out List<WorfRelation>? entries))
        {
            _ = entries.Remove(old);
        }

        // Make cached copy of the data
        hash[remoteid] = new WorfRelation
        {
            RemoteId = remoteid,
            Nspname = schemaname,
            Relname = relname,
            Attnames = attnames,
            Attmap = new int[attnames.Length],
            RemoteTypes = types ?? new WorfRemoteType?[attnames.Length],
            Conversions = new WorfColumnConversion?[attnames.Length],
            Reloid = 0,
        };
    }

    /// <summary>
    /// Records a remote relation's description from the information function.
    /// </summary>
    /// <param name="remoterel">The remote relation.</param>
    internal static void RelationCacheUpdater(WorfRemoteRel remoterel)
        => RelationCacheUpdate(remoterel.Relid, remoterel.Nspname, remoterel.Relname, [.. remoterel.Attnames]);

    /// <summary>
    /// Closes the local relation. <see cref="PgLockMode.None"/> keeps the lock its open took until the transaction
    /// ends; another mode releases it.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="lockmode">The lock to release, or <see cref="PgLockMode.None"/>.</param>
    internal static unsafe void RelationClose(WorfRelation rel, PgLockMode lockmode)
    {
        if (rel.Rel is not null)
        {
            NativeMethods.table_close(rel.Rel, (int)lockmode);
        }

        rel.Rel = null;
    }

    /// <summary>
    /// Forgets the local relations that are open, after the subtransaction that opened them rolled back and released
    /// them.
    /// </summary>
    internal static unsafe void ForgetOpenRelations()
    {
        if (s_relationHash is null)
        {
            return;
        }

        foreach (WorfRelation rel in s_relationHash.Values)
        {
            rel.Rel = null;
        }
    }

    private static void RelcacheInvalidateCallback(ulong arg, uint reloid)
    {
        _ = arg;

        // Just to be sure.
        if (s_relationHash is null)
        {
            return;
        }

        if (reloid != 0)
        {
            if (s_relationsByReloid.Remove(reloid, out List<WorfRelation>? entries))
            {
                foreach (WorfRelation entry in entries)
                {
                    entry.Reloid = 0;
                }
            }
        }
        else
        {
            // invalidate all cache entries
            foreach (WorfRelation entry in s_relationHash.Values)
            {
                entry.Reloid = 0;
            }

            s_relationsByReloid.Clear();
        }
    }

    private static Dictionary<uint, WorfRelation> RelcacheInit()
    {
        // Watch for invalidation events.
        unsafe
        {
            NativeMethods.CacheRegisterRelcacheCallback(InvalidateCallback, 0);
        }

        return [];
    }

    /// <summary>
    /// Find attribute index in TupleDesc struct by attribute name.
    /// </summary>
    private static unsafe int TupdescGetAttByName(TupleDescData* desc, string attname)
    {
        for (int i = 0; i < desc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, i);

            if (Worf.FromServer((sbyte*)&att->attname) == attname)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// Finds how a remote column's values convert to the local column's type, when they're sent in another: they're
    /// read with PostgreSQL's own types by OID, and with others found by schema and name when defined the same here.
    /// Values of a type found neither way are read as the local column's own type when that's defined the same, as
    /// the same type under another schema or name.
    /// </summary>
    /// <param name="entry">The remote relation.</param>
    /// <param name="remote">The type the column's values are sent in.</param>
    /// <param name="att">The local column.</param>
    /// <returns>The conversion, or null when the values are of the local column's type.</returns>
    private static unsafe WorfColumnConversion? ColumnConversion(WorfRelation entry, WorfRemoteType remote, FormData_pg_attribute* att)
    {
        string column = Worf.FromServer((sbyte*)&att->attname);
        uint local = NativeMethods.getBaseType(att->atttypid);
        if (remote.Definition is null)
        {
            if (remote.Oid == local)
            {
                return null;
            }

            return NativeMethods.get_typtype(remote.Oid) != 0
                ? new WorfColumnConversion(column, Worf.FromServer(NativeMethods.format_type_be(remote.Oid)), remote.Oid)
                : new WorfColumnConversion(PgSqlStates.UndefinedObject,
                    $"type with OID {remote.Oid} of column {column} of remote table {entry.Nspname}.{entry.Relname} does not exist");
        }

        string name = $"{remote.Nspname}.{remote.Typname}";
        string localDefinition = ProtoNative.TypeDefinition(local);
        if (localDefinition == remote.Definition && TypeName(local) == name)
        {
            return null;
        }

        SpiResult found = Worf.SelectCatalog("""
            SELECT t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = $1 AND t.typname = $2
            """, SpiParameter.Create(remote.Nspname), SpiParameter.Create(remote.Typname));
        if (found.Count != 0 && ProtoNative.TypeDefinition(found[0].Get<uint>(0)) == remote.Definition)
        {
            uint readType = NativeMethods.getBaseType(found[0].Get<uint>(0));
            return readType == local ? null : new WorfColumnConversion(column, name, readType);
        }

        if (localDefinition == remote.Definition)
        {
            return null;
        }

        return found.Count == 0
            ? new WorfColumnConversion(PgSqlStates.UndefinedObject,
                $"type {name} of column {column} of remote table {entry.Nspname}.{entry.Relname} does not exist")
            : new WorfColumnConversion(PgSqlStates.DatatypeMismatch,
                $"type {name} of column {column} of remote table {entry.Nspname}.{entry.Relname} is defined differently here");
    }

    /// <summary>
    /// Gets a type's schema and name.
    /// </summary>
    private static unsafe string TypeName(uint typoid)
    {
        HeapTupleData* typtup = NativeMethods.typeidType(typoid);
        FormData_pg_type* typclass = (FormData_pg_type*)NativeMethods.GETSTRUCT(typtup);
        sbyte* nspname = NativeMethods.get_namespace_name(typclass->typnamespace);
        string name = $"{(nspname is null ? "" : Worf.FromServer(nspname))}.{Worf.FromServer((sbyte*)&typclass->typname)}";
        NativeMethods.ReleaseSysCache(typtup);
        return name;
    }

    private static bool ColumnDropped(uint relid, string attname)
        => Worf.SelectCatalog("""
            SELECT 1 FROM worf.dropped_column d
                JOIN pg_catalog.pg_attribute a ON a.attrelid = d.relid AND a.attnum = d.attnum AND a.attisdropped
            WHERE d.relid = $1 AND d.attname = $2::name
            """, SpiParameter.Create(relid), SpiParameter.Create(attname)).Count > 0;

    private const sbyte TriggerFiresOnOrigin = (sbyte)'O';
    private const sbyte TriggerDisabled = (sbyte)'D';
    private const int TriggerTypeRow = 1;
}
