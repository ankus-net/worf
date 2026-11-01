using System.Globalization;
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

#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    // TYPEOID and PROCOID, the ids of the catalog caches of types and functions, which only PostgreSQL 19's bindings
    // name.
    private const int TypeCacheId = 80;
    private const int ProcCacheId = 45;
#elif ANKUS_PG18
    private const int TypeCacheId = 82;
    private const int ProcCacheId = 47;
#endif

    /// <summary>
    /// Counts the changes to types and functions this backend has seen, which the column conversions depend on.
    /// </summary>
    private static long s_typesStamp;

    /// <summary>
    /// Gets or sets how the provider formats money, as <see cref="OutputConfig.MoneyFormat"/> shows it, or null when it
    /// didn't say. Values sent as money fail when this node formats it differently, under another <c>lc_monetary</c>,
    /// since they count the provider's currency in its fraction digits.
    /// </summary>
    internal static string? ProviderMoneyFormat { get; set; }

    [PgNativeCallback(nameof(RelcacheInvalidateCallback))]
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static partial RelcacheCallbackFunction InvalidateCallback { get; }
#else
    private static partial RelSyncCallbackFunction InvalidateCallback { get; }
#endif

    [PgNativeCallback(nameof(TypeInvalidateCallback))]
    private static partial SyscacheCallbackFunction TypeCallback { get; }

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
        bool mapped = entry.Reloid == 0;
        if (mapped)
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

        // The conversions are found again for the local columns, and after a change to the types and functions they
        // depend on.
        if (mapped || (entry.ConversionsDependOnTypes && entry.ConversionsStamp != s_typesStamp))
        {
            FindConversions(entry);
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

        if (hash.TryGetValue(remoteid, out WorfRelation? old))
        {
            if (s_relationsByReloid.TryGetValue(old.Reloid, out List<WorfRelation>? entries))
            {
                _ = entries.Remove(old);
            }

            ReleaseConversions(old);
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
    /// <param name="types">The remote columns' types, or null when the provider didn't say.</param>
    internal static void RelationCacheUpdater(WorfRemoteRel remoterel, WorfRemoteType?[]? types)
        => RelationCacheUpdate(remoterel.Relid, remoterel.Nspname, remoterel.Relname, [.. remoterel.Attnames], types);

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

    /// <summary>
    /// Notes a change to a type or function, after which the conversions that depend on them are found again.
    /// </summary>
#if ANKUS_PG19
    private static void TypeInvalidateCallback(ulong arg, SysCacheIdentifier cacheid, uint hashvalue)
#else
    private static void TypeInvalidateCallback(ulong arg, int cacheid, uint hashvalue)
#endif
    {
        _ = arg;
        _ = cacheid;
        _ = hashvalue;
        s_typesStamp++;
    }

    private static Dictionary<uint, WorfRelation> RelcacheInit()
    {
        // Watch for invalidation events.
        unsafe
        {
            NativeMethods.CacheRegisterRelcacheCallback(InvalidateCallback, 0);
#if ANKUS_PG19
            NativeMethods.CacheRegisterSyscacheCallback(SysCacheIdentifier.TYPEOID, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(SysCacheIdentifier.PROCOID, TypeCallback, 0);
#else
            NativeMethods.CacheRegisterSyscacheCallback(TypeCacheId, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(ProcCacheId, TypeCallback, 0);
#endif
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
    /// Finds how each remote column's values convert to its local column's type, and releases the conversions found
    /// before.
    /// </summary>
    private static unsafe void FindConversions(WorfRelation entry)
    {
        // Conversions left half found by an error are found again on the next open.
        long stamp = s_typesStamp;
        bool dependOnTypes = false;
        entry.ConversionsDependOnTypes = true;
        entry.ConversionsStamp = -1;
        TupleDescData* desc = entry.Rel->rd_att;
        for (int i = 0; i < entry.Natts; i++)
        {
            entry.Conversions[i]?.Release();
            entry.Conversions[i] = null;
            if (entry.Attmap[i] >= 0 && entry.RemoteTypes[i] is WorfRemoteType remote)
            {
                entry.Conversions[i] = ColumnConversion(remote, NativeMethods.TupleDescAttr(desc, entry.Attmap[i]));
                dependOnTypes |= remote.Definition is not null || entry.Conversions[i] is not null;
            }
        }

        entry.ConversionsDependOnTypes = dependOnTypes;
        entry.ConversionsStamp = stamp;
    }

    private static void ReleaseConversions(WorfRelation entry)
    {
        foreach (WorfColumnConversion? conversion in entry.Conversions)
        {
            conversion?.Release();
        }
    }

    /// <summary>
    /// Finds how a remote column's values convert to the local column's type, when they're sent in another: PostgreSQL's
    /// own types are read by OID, and others found by schema and name when defined the same. An enum's values are its
    /// labels, which an enum column reads and another reads as text, and a type made of enums is read only for a
    /// column whose type is made of the same ones, since the provider's labels may be ones they lack. A type the
    /// subscriber lacks or defines differently, or made of enums the column's type isn't, can't be read in binary, and
    /// the column's own type reads its text. Values sent as money fail when the provider formats money differently.
    /// </summary>
    /// <param name="remote">The type the column's values are sent in.</param>
    /// <param name="att">The local column.</param>
    /// <returns>The conversion, or null when the local column's type reads the values.</returns>
    private static unsafe WorfColumnConversion? ColumnConversion(WorfRemoteType remote, FormData_pg_attribute* att)
    {
        uint local = NativeMethods.getBaseType(att->atttypid);
        uint readType = 0;
        string name;
        if (SendsMoney(remote) && ProviderMoneyFormat is string providerFormat && providerFormat != OutputConfig.MoneyFormat())
        {
            string column = Worf.FromServer((sbyte*)&att->attname);
            return new WorfColumnConversion(column, "money", 0, att->atttypid, att->atttypmod,
                $"column \"{column}\" gets money from a provider whose lc_monetary formats it as \"{providerFormat}\" where this node's formats it as \"{OutputConfig.MoneyFormat()}\"");
        }

        if (remote.Definition is null)
        {
            if (remote.Oid == local)
            {
                return null;
            }

            if (Readable(remote.Oid))
            {
                readType = remote.Oid;
                name = Worf.FromServer(NativeMethods.format_type_be(readType));
            }
            else
            {
                name = remote.Oid.ToString(CultureInfo.InvariantCulture);
            }
        }
        else
        {
            name = $"{remote.Nspname}.{remote.Typname}";
            if (remote.Definition == ProtoNative.EnumDefinition)
            {
                if (NativeMethods.get_typtype(local) == EnumType)
                {
                    return null;
                }

                readType = WorfColumnConversion.TextOid;
            }
            else if (ProtoNative.TypeDefinition(local) == remote.Definition)
            {
                return null;
            }
            else
            {
                SpiResult found = Worf.SelectCatalog("""
                    SELECT t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                    WHERE n.nspname = $1 AND t.typname = $2
                    """, SpiParameter.Create(remote.Nspname), SpiParameter.Create(remote.Typname));
                if (found.Count > 0 && Readable(found[0].Get<uint>(0)) &&
                    ProtoNative.TypeDefinition(found[0].Get<uint>(0)) == remote.Definition &&
                    MadeOfEnumsOf(att->atttypid, found[0].Get<uint>(0)))
                {
                    readType = NativeMethods.getBaseType(found[0].Get<uint>(0));
                }
            }
        }

        return new WorfColumnConversion(Worf.FromServer((sbyte*)&att->attname), name, readType, att->atttypid, att->atttypmod);
    }

    /// <summary>
    /// Checks whether a type is made of only enums another type is made of too.
    /// </summary>
    /// <param name="column">The type that has to be made of the enums.</param>
    /// <param name="type">The type whose enums those are.</param>
    private static bool MadeOfEnumsOf(uint column, uint type)
    {
        HashSet<uint> enums = [];
        CollectEnums(type, enums);
        if (enums.Count == 0)
        {
            return true;
        }

        HashSet<uint> columnEnums = [];
        CollectEnums(column, columnEnums);
        return enums.IsSubsetOf(columnEnums);
    }

    /// <summary>
    /// Collects the enums a type is made of: itself, its base type's, its element's, its subtype's or its attributes'.
    /// </summary>
    private static unsafe void CollectEnums(uint typoid, HashSet<uint> enums)
    {
        typoid = NativeMethods.getBaseType(typoid);
        uint element = NativeMethods.get_element_type(typoid);
        if (element != 0)
        {
            CollectEnums(element, enums);
            return;
        }

        switch (NativeMethods.get_typtype(typoid))
        {
            case EnumType:
                _ = enums.Add(typoid);
                break;

            case RangeType:
                CollectEnums(NativeMethods.get_range_subtype(typoid), enums);
                break;

            case MultirangeType:
                CollectEnums(NativeMethods.get_multirange_range(typoid), enums);
                break;

            case CompositeType:
            {
                TupleDescData* desc = NativeMethods.lookup_rowtype_tupdesc(typoid, -1);
                for (int i = 0; i < desc->natts; i++)
                {
                    FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, i);
                    if (!att->attisdropped)
                    {
                        CollectEnums(att->atttypid, enums);
                    }
                }

                if (desc->tdrefcount >= 0)
                {
                    NativeMethods.DecrTupleDescRefCount(desc);
                }

                break;
            }
        }
    }

    /// <summary>
    /// Checks whether values sent in a type are money or made of it, as the type's OID or definition shows.
    /// </summary>
    /// <param name="remote">The type.</param>
    /// <returns>Whether they are.</returns>
    internal static bool SendsMoney(WorfRemoteType remote)
        => remote.Definition is null
            ? remote.Oid is MoneyOid or MoneyArrayOid
            : remote.Definition.Split('(', ')', ',').Any(part => part is MoneyOidText or MoneyArrayOidText);

    /// <summary>
    /// Checks whether a type exists here and its values can be read, which a shell or pseudo-type's can't.
    /// </summary>
    private static bool Readable(uint typoid)
    {
        unsafe
        {
            return NativeMethods.get_typisdefined(typoid) && NativeMethods.get_typtype(typoid) != (sbyte)'p';
        }
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
    private const sbyte EnumType = (sbyte)'e';
    private const sbyte RangeType = (sbyte)'r';
    private const sbyte MultirangeType = (sbyte)'m';
    private const sbyte CompositeType = (sbyte)'c';

    // The OIDs of money and its array, also as type definitions write them.
    private const uint MoneyOid = 790;
    private const uint MoneyArrayOid = 791;
    private const string MoneyOidText = "790";
    private const string MoneyArrayOidText = "791";
}
