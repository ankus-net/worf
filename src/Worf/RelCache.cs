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

    // The lc_monetary that the relations' conversions were found under.
    private static string? s_monetary;

    [PgNativeCallback(nameof(RelcacheInvalidateCallback))]
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static partial RelcacheCallbackFunction InvalidateCallback { get; }
#else
    private static partial RelSyncCallbackFunction InvalidateCallback { get; }
#endif

    [PgNativeCallback(nameof(TypeInvalidateCallback))]
    private static partial SyscacheCallbackFunction TypeCallback { get; }

    /// <summary>
    /// Gets the schema and name of a remote relation, without opening the local relation it maps to.
    /// </summary>
    /// <param name="remoteid">The remote relation's id.</param>
    /// <returns>The schema and the name, or nulls when the provider hasn't described the relation.</returns>
    internal static (string? Nspname, string? Relname) RemoteName(uint remoteid)
        => s_relationHash is not null && s_relationHash.TryGetValue(remoteid, out WorfRelation? entry) ? (entry.Nspname, entry.Relname) : (null, null);

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

        // Money's conversions depend on how this node formats it, which a reload of lc_monetary changes, so every
        // relation's are found again then.
        if (OutputConfig.MonetaryChanged(ref s_monetary))
        {
            foreach (WorfRelation other in hash.Values)
            {
                other.IsValid = false;
            }

            s_relationsByReloid.Clear();
        }

        // Locking the relation takes the invalidations sent since it was last open, which can leave the mapping stale,
        // as when the table was changed, renamed or dropped. It's then let go and looked up by name again. Its
        // invalidation can also come while a change uses it, so it keeps its OID until then.
        if (entry.IsValid && entry.Rel is null)
        {
            entry.Rel = NativeMethods.try_table_open(entry.Reloid, (int)lockmode);
            entry.IsValid &= entry.Rel is not null;
        }

        if (entry.Rel is not null && !entry.IsValid)
        {
            NativeMethods.table_close(entry.Rel, (int)lockmode);
            entry.Rel = null;
        }

        // Need to update the local cache?
        bool mapped = !entry.IsValid;
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
            entry.IsValid = true;
            if (!s_relationsByReloid.TryGetValue(entry.Reloid, out List<WorfRelation>? entries))
            {
                entries = [];
                s_relationsByReloid.Add(entry.Reloid, entries);
            }

            entries.Add(entry);

            entry.DeferrableIndexes = Worf.ReadScalar<string?>("""
                SELECT pg_catalog.string_agg(pg_catalog.quote_ident(c.relname), ', ' ORDER BY c.relname)
                FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = $1 AND NOT i.indimmediate AND (i.indexprs IS NOT NULL OR i.indpred IS NOT NULL)
                """, SpiParameter.Create(entry.Reloid));
            entry.DeferrableChecks = DeferrableChecks(entry);

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

        // The conversions are found again for the local columns, and after a change to the types and functions they
        // depend on.
        if (mapped || (entry.ConversionsDependOnTypes && entry.ConversionsStamp != s_typesStamp))
        {
            FindConversions(entry);
        }

        return entry;
    }

    /// <summary>
    /// Fails an insert or update into a relation with deferrable indexes whose rechecks apply wouldn't get, and can't
    /// do itself, which would let it commit rows that conflict in them.
    /// </summary>
    /// <param name="rel">The relation.</param>
    internal static void CheckDeferrableIndexes(WorfRelation rel)
    {
        if (rel.DeferrableIndexes is not null)
        {
            throw new PgException(PgSqlStates.FeatureNotSupported, "worf doesn't support index rechecks needed for deferrable indexes with expressions or a predicate",
                detail: $"relation {Worf.QuoteQualifiedIdentifier(rel.Nspname, rel.Relname)} has deferrable indexes: {rel.DeferrableIndexes}",
                hint: "Make the constraints not deferrable: ALTER TABLE ... ALTER CONSTRAINT ... NOT DEFERRABLE for a unique one, and for an exclusion constraint, drop it and add it again.");
        }
    }

    /// <summary>
    /// Builds the checks of a relation's deferrable unique and exclusion constraints on its columns, which apply runs
    /// after it writes a row, as their rechecks would at commit: whether another row has the same key, or one that the
    /// constraint's operators find conflicting, as a row conflicts with itself when they find it so.
    /// </summary>
    private static List<(string Name, bool Exclusion, string Query, int[] Columns)>? DeferrableChecks(WorfRelation entry)
    {
        WorfRows constraints = Worf.Read("""
            SELECT c.conname::text, c.contype = 'x', i.indnullsnotdistinct,
                pg_catalog.array_agg(k.attnum::int4 ORDER BY k.n),
                pg_catalog.array_agg(pg_catalog.quote_ident(a.attname) ORDER BY k.n),
                pg_catalog.array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY k.n),
                pg_catalog.array_agg('OPERATOR(' || pg_catalog.quote_ident(n.nspname) || '.' || o.oprname || ')' ORDER BY k.n)
            FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_constraint c ON c.conindid = i.indexrelid AND c.conrelid = i.indrelid
                CROSS JOIN LATERAL ROWS FROM (pg_catalog.unnest(i.indkey::pg_catalog.int2[]), pg_catalog.unnest(i.indclass::pg_catalog.oid[]))
                    WITH ORDINALITY AS k (attnum, opclass, n)
                JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                JOIN pg_catalog.pg_opclass oc ON oc.oid = k.opclass
                JOIN pg_catalog.pg_operator o ON o.oid = CASE WHEN c.contype = 'x' THEN c.conexclop[k.n]
                    ELSE (SELECT p.amopopr FROM pg_catalog.pg_amop p
                        WHERE p.amopfamily = oc.opcfamily AND p.amoplefttype = oc.opcintype AND p.amoprighttype = oc.opcintype AND p.amopstrategy = 3) END
                JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace
            WHERE i.indrelid = $1 AND NOT i.indimmediate AND i.indexprs IS NULL AND i.indpred IS NULL AND k.n <= i.indnkeyatts
            GROUP BY c.conname, c.contype, i.indnullsnotdistinct
            """, 0, SpiParameter.Create(entry.Reloid));
        if (constraints.Count == 0)
        {
            return null;
        }

        string table = Worf.QuoteQualifiedIdentifier(entry.Nspname, entry.Relname);
        List<(string Name, bool Exclusion, string Query, int[] Columns)> checks = [];
        foreach (WorfRow constraint in constraints)
        {
            bool nullsEqual = constraint.Get<bool>(2);
            int[] columns = constraint.Get<int[]>(3);
            string[] names = constraint.Get<string[]>(4);
            string[] types = constraint.Get<string[]>(5);
            string[] operators = constraint.Get<string[]>(6);
            List<string> others = [];
            List<string> self = [];
            for (int k = 0; k < columns.Length; k++)
            {
                string value = $"pg_catalog.convert_from(${k + 1}, pg_catalog.getdatabaseencoding())::{types[k]}";
                others.Add(nullsEqual ? $"({names[k]} {operators[k]} {value} OR ({names[k]} IS NULL AND ${k + 1} IS NULL))" : $"{names[k]} {operators[k]} {value}");
                self.Add(nullsEqual ? $"({value} {operators[k]} {value} OR ${k + 1} IS NULL)" : $"{value} {operators[k]} {value}");
            }

            checks.Add((constraint.Get<string>(0), constraint.Get<bool>(1),
                $"SELECT (SELECT pg_catalog.count(*) FROM ONLY {table} WHERE {string.Join(" AND ", others)}) > CASE WHEN {string.Join(" AND ", self)} THEN 1 ELSE 0 END",
                columns));
        }

        return checks;
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
                    entry.IsValid = false;
                }
            }
        }
        else
        {
            // invalidate all cache entries
            foreach (WorfRelation entry in s_relationHash.Values)
            {
                entry.IsValid = false;
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
        // Money's binary form counts the smallest units of the currency, so values sent so mean the same only while both
        // nodes count the same fraction digits, and are read as what they're sent in, then converted as any value is,
        // for a column of another type, and its text, as the copy and text transfer send, means the same only where it
        // reads here as the same amount.
        if (SendsMoney(remote) && ProviderMoneyFormat is string providerFormat && MoneyAlike(providerFormat) is bool binaryAlike)
        {
            string column = Worf.FromServer((sbyte*)&att->attname);
            string failure = $"column \"{column}\" gets money from a provider whose lc_monetary formats it as \"{providerFormat}\" where this node's formats it as \"{OutputConfig.MoneyFormat()}\"";
            if (!binaryAlike)
            {
                return new WorfColumnConversion(column, "money", 0, att->atttypid, att->atttypmod, failure);
            }

            failure += ", which reads as another amount, or none, in the text the copy and text transfer send";
            return ConvertedColumn(remote, att) is WorfColumnConversion converted
                ? converted.FailingText(failure)
                : new WorfColumnConversion(column, "money", 0, att->atttypid, att->atttypmod, failure, true);
        }

        return ConvertedColumn(remote, att);
    }

    /// <summary>
    /// Finds how a remote column's values convert to the local column's type, as <see cref="ColumnConversion"/> does,
    /// whatever the nodes' lc_monetary.
    /// </summary>
    private static unsafe WorfColumnConversion? ConvertedColumn(WorfRemoteType remote, FormData_pg_attribute* att)
    {
        uint local = NativeMethods.getBaseType(att->atttypid);
        uint readType = 0;
        string name;
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
                WorfRows found = Worf.SelectCatalog("""
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
    /// Checks how far money the provider formats one way means here what it means there: in binary, its values count
    /// the same fraction digits; as text, which the copy and text transfer send, its text for an amount, with the
    /// separators and the sign, reads here as the same amount, where a text that fails to read, or reads as another
    /// amount, means that other values may read as amounts a hundred times off.
    /// </summary>
    /// <param name="providerFormat">How the provider formats money, as <see cref="OutputConfig.MoneyFormat"/> shows it.</param>
    /// <returns>Null when it means the same either way, true when it does only in binary, or false when it doesn't.</returns>
    internal static bool? MoneyAlike(string providerFormat)
    {
        string local = OutputConfig.MoneyFormat();
        if (providerFormat == local)
        {
            return null;
        }

        if (OutputConfig.MoneyAmount(providerFormat) != OutputConfig.MoneyAmount(local))
        {
            return false;
        }

        try
        {
            return PgTransaction.RunInSubtransaction(() => Worf.ReadScalar<bool>(
                "SELECT $1::pg_catalog.money OPERATOR(pg_catalog.=) '-1234567'::pg_catalog.money",
                SpiParameter.Create(OutputConfig.MoneyText(providerFormat))), PgSubtransactionMode.Atomic) ? null : true;
        }
        catch (PgException)
        {
            return true;
        }
    }

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
