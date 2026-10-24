using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf JSON protocol functions.
/// </summary>
internal static unsafe class ProtoJson
{
    // Type oids from catalog/pg_type_d.h.
    private const uint BoolOid = 16;
    private const uint Int8Oid = 20;
    private const uint Int2Oid = 21;
    private const uint Int4Oid = 23;
    private const uint JsonOid = 114;
    private const uint Float4Oid = 700;
    private const uint Float8Oid = 701;
    private const uint DateOid = 1082;
    private const uint TimestampOid = 1114;
    private const uint TimestampTzOid = 1184;
    private const uint NumericOid = 1700;
    private const uint AnyArrayOid = 2277;
    private const uint RecordArrayOid = 2287;
    private const uint JsonbOid = 3802;
    private const uint FirstNormalObjectId = 16384;

    // array_to_json(anyarray) and row_to_json(record), which read their argument's type from the datum.
    private const uint ArrayToJsonOid = 3153;
    private const uint RowToJsonOid = 3155;

    // RBTXN_HAS_CATALOG_CHANGES from replication/reorderbuffer.h.
    private const uint RbtxnHasCatalogChanges = 0x0001;

    /// <summary>
    /// Write BEGIN to the output stream.
    /// </summary>
    internal static void WriteBegin(StringInfoData* @out, WorfOutputData data, ReorderBufferTXN* txn)
    {
        Append(@out, "{");
        Append(@out, "\"action\":\"B\"");
        Append(@out, $", \"has_catalog_changes\":\"{((txn->txn_flags & RbtxnHasCatalogChanges) != 0 ? 't' : 'f')}\"");
        if (txn->origin_id != 0)
        {
            Append(@out, $", \"origin_id\":\"{txn->origin_id}\"");
        }

        if (!data.ClientNoTxinfo)
        {
            Append(@out, $", \"xid\":\"{txn->xid}\"");
            Append(@out, $", \"first_lsn\":\"{Worf.FormatLsn(txn->first_lsn)}\"");
            Append(@out, $", \"origin_lsn\":\"{Worf.FormatLsn(txn->origin_lsn)}\"");
            long commitTime = CommitTime(txn);
            if (commitTime != 0)
            {
                Append(@out, $", \"commit_time\":\"{Worf.FromServer(NativeMethods.timestamptz_to_str(commitTime))}\"");
            }
        }

        Append(@out, "}");
    }

    /// <summary>
    /// Write COMMIT to the output stream.
    /// </summary>
    internal static void WriteCommit(StringInfoData* @out, WorfOutputData data, ReorderBufferTXN* txn, ulong commitLsn)
    {
        _ = commitLsn;
        Append(@out, "{");
        Append(@out, "\"action\":\"C\"");
        if (!data.ClientNoTxinfo)
        {
            Append(@out, $", \"final_lsn\":\"{Worf.FormatLsn(txn->final_lsn)}\"");
            Append(@out, $", \"end_lsn\":\"{Worf.FormatLsn(txn->end_lsn)}\"");
        }

        Append(@out, "}");
    }

    /// <summary>
    /// Write INSERT to the output stream.
    /// </summary>
    internal static void WriteInsert(StringInfoData* @out, WorfOutputData data, RelationData* rel, HeapTupleData* newtuple,
        HashSet<int>? attList)
    {
        _ = data;
        WriteChange(@out, "I", rel, null, newtuple, attList);
    }

    /// <summary>
    /// Write UPDATE to the output stream.
    /// </summary>
    internal static void WriteUpdate(StringInfoData* @out, WorfOutputData data, RelationData* rel, HeapTupleData* oldtuple,
        HeapTupleData* newtuple, HashSet<int>? attList)
    {
        _ = data;
        WriteChange(@out, "U", rel, oldtuple, newtuple, attList);
    }

    /// <summary>
    /// Write DELETE to the output stream.
    /// </summary>
    internal static void WriteDelete(StringInfoData* @out, WorfOutputData data, RelationData* rel, HeapTupleData* oldtuple,
        HashSet<int>? attList)
    {
        _ = data;
        WriteChange(@out, "D", rel, oldtuple, null, attList);
    }

    /// <summary>
    /// Write the startup message as a JSON object of its parameters.
    /// </summary>
    internal static void WriteStartupMessage(StringInfoData* @out, List<(string Key, string Value)> msg)
    {
        bool first = true;

        Append(@out, "{\"action\":\"S\", \"params\": {");
        foreach ((string key, string value) in msg)
        {
            if (first)
            {
                first = false;
            }
            else
            {
                Append(@out, ",");
            }

            EscapeJson(@out, key);
            Append(@out, ":");
            EscapeJson(@out, value);
        }

        Append(@out, "}}");
    }

    private static void WriteChange(StringInfoData* @out, string change, RelationData* rel, HeapTupleData* oldtuple,
        HeapTupleData* newtuple, HashSet<int>? attList)
    {
        Append(@out, "{");
        Append(@out, $"\"action\":\"{change}\",\"relation\":[\"");
        NativeMethods.appendStringInfoString(@out, NativeMethods.get_namespace_name(rel->rd_rel->relnamespace));
        Append(@out, "\",\"");
        NativeMethods.appendStringInfoString(@out, (sbyte*)&rel->rd_rel->relname);
        Append(@out, "\"]");

        if (oldtuple is not null)
        {
            Append(@out, ",\"oldtuple\":");
            JsonWriteTuple(@out, rel, oldtuple, attList);
        }

        if (newtuple is not null)
        {
            Append(@out, ",\"newtuple\":");
            JsonWriteTuple(@out, rel, newtuple, attList);
        }

        Append(@out, "}");
    }

    private static void JsonWriteTuple(StringInfoData* @out, RelationData* rel, HeapTupleData* tuple, HashSet<int>? attList)
    {
        TupleDescData* tupdesc = rel->rd_att;
        bool needsep = false;
        ulong* values = (ulong*)NativeMethods.palloc((ulong)(tupdesc->natts * sizeof(ulong)));
        bool* isnull = (bool*)NativeMethods.palloc((ulong)tupdesc->natts);

        Append(@out, "{");

        NativeMethods.heap_deform_tuple(tuple, tupdesc, values, isnull);

        for (int i = 0; i < tupdesc->natts; i++)
        {
            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(tupdesc, i);
            JsonTypeCategory tcategory;
            uint outfuncoid;
            uint typoid;

            // skip dropped columns
            if (att->attisdropped)
            {
                continue;
            }

            if (attList is not null && !attList.Contains(att->attnum))
            {
                continue;
            }

            // Don't send unchanged toast column as we may not be able to fetch them.
            if (!isnull[i] && att->attlen == -1 && Varlena.IsExternalOndisk(values[i]))
            {
                continue;
            }

            if (needsep)
            {
                Append(@out, ",");
            }

            needsep = true;

            NativeMethods.escape_json(@out, (sbyte*)&att->attname);
            Append(@out, ":");

            if (isnull[i])
            {
                tcategory = JsonTypeCategory.Null;
                outfuncoid = 0;
                typoid = 0;
            }
            else
            {
                JsonCategorizeType(att->atttypid, out tcategory, out outfuncoid, out typoid);
            }

            DatumToJson(values[i], isnull[i], @out, tcategory, outfuncoid, typoid);
        }

        Append(@out, "}");

        NativeMethods.pfree(values);
        NativeMethods.pfree(isnull);
    }

    private static void JsonCategorizeType(uint typoid, out JsonTypeCategory tcategory, out uint outfuncoid, out uint basetypoid)
    {
        bool typisvarlena;
        uint funcoid = 0;

        // Look through any domain
        typoid = NativeMethods.getBaseType(typoid);
        basetypoid = typoid;

        outfuncoid = 0;

        // We need to get the output function for everything except date and timestamp types, array and composite
        // types, booleans, and non-builtin types where there's a cast to json.
        switch (typoid)
        {
            case BoolOid:
                tcategory = JsonTypeCategory.Bool;
                break;

            case Int2Oid:
            case Int4Oid:
            case Int8Oid:
            case Float4Oid:
            case Float8Oid:
            case NumericOid:
                NativeMethods.getTypeOutputInfo(typoid, &funcoid, &typisvarlena);
                outfuncoid = funcoid;
                tcategory = JsonTypeCategory.Numeric;
                break;

            case DateOid:
                tcategory = JsonTypeCategory.Date;
                break;

            case TimestampOid:
                tcategory = JsonTypeCategory.Timestamp;
                break;

            case TimestampTzOid:
                tcategory = JsonTypeCategory.TimestampTz;
                break;

            case JsonOid:
            case JsonbOid:
                NativeMethods.getTypeOutputInfo(typoid, &funcoid, &typisvarlena);
                outfuncoid = funcoid;
                tcategory = JsonTypeCategory.Json;
                break;

            default:
                // Check for arrays and composites
                if (NativeMethods.get_element_type(typoid) != 0 || typoid == AnyArrayOid || typoid == RecordArrayOid)
                {
                    tcategory = JsonTypeCategory.Array;
                }
                else if (NativeMethods.type_is_rowtype(typoid))
                {
                    // includes RECORDOID
                    tcategory = JsonTypeCategory.Composite;
                }
                else
                {
                    // It's probably the general case ...
                    tcategory = JsonTypeCategory.Other;

                    // but let's look for a cast to json, if it's not built-in
                    if (typoid >= FirstNormalObjectId)
                    {
                        uint castfunc;
                        CoercionPathType ctype = NativeMethods.find_coercion_pathway(JsonOid, typoid,
                            CoercionContext.COERCION_EXPLICIT, &castfunc);
                        if (ctype == CoercionPathType.COERCION_PATH_FUNC && castfunc != 0)
                        {
                            tcategory = JsonTypeCategory.Cast;
                            outfuncoid = castfunc;
                        }
                        else
                        {
                            // non builtin type with no cast
                            NativeMethods.getTypeOutputInfo(typoid, &funcoid, &typisvarlena);
                            outfuncoid = funcoid;
                        }
                    }
                    else
                    {
                        // any other builtin type
                        NativeMethods.getTypeOutputInfo(typoid, &funcoid, &typisvarlena);
                        outfuncoid = funcoid;
                    }
                }

                break;
        }
    }

    private static void DatumToJson(ulong val, bool isNull, StringInfoData* result, JsonTypeCategory tcategory,
        uint outfuncoid, uint typoid)
    {
        if (isNull)
        {
            Append(result, "null");
            return;
        }

        switch (tcategory)
        {
            case JsonTypeCategory.Array:
                AppendText(result, NativeMethods.OidFunctionCall1Coll(ArrayToJsonOid, 0, val));
                break;

            case JsonTypeCategory.Composite:
                AppendText(result, NativeMethods.OidFunctionCall1Coll(RowToJsonOid, 0, val));
                break;

            case JsonTypeCategory.Bool:
                Append(result, (val & 0xFF) != 0 ? "true" : "false");
                break;

            case JsonTypeCategory.Numeric:
            {
                sbyte* outputstr = NativeMethods.OidOutputFunctionCall(outfuncoid, val);

                // Don't call escape_json for a non-key if it's a valid JSON number.
                if (IsValidJsonNumber(Worf.FromServer(outputstr)))
                {
                    NativeMethods.appendStringInfoString(result, outputstr);
                }
                else
                {
                    NativeMethods.escape_json(result, outputstr);
                }

                NativeMethods.pfree(outputstr);
                break;
            }

            case JsonTypeCategory.Date:
            case JsonTypeCategory.Timestamp:
            case JsonTypeCategory.TimestampTz:
            {
                // Same as date_out() or timestamp_out(), but forcing DateStyle
                sbyte* buf = NativeMethods.JsonEncodeDateTime(null, val, typoid, null);
                Append(result, "\"");
                NativeMethods.appendStringInfoString(result, buf);
                Append(result, "\"");
                break;
            }

            case JsonTypeCategory.Json:
            {
                // JSON and JSONB output will already be escaped
                sbyte* outputstr = NativeMethods.OidOutputFunctionCall(outfuncoid, val);
                NativeMethods.appendStringInfoString(result, outputstr);
                NativeMethods.pfree(outputstr);
                break;
            }

            case JsonTypeCategory.Cast:
                // outfuncoid refers to a cast function, not an output function
                AppendText(result, NativeMethods.OidFunctionCall1Coll(outfuncoid, 0, val));
                break;

            default:
            {
                sbyte* outputstr = NativeMethods.OidOutputFunctionCall(outfuncoid, val);
                NativeMethods.escape_json(result, outputstr);
                NativeMethods.pfree(outputstr);
                break;
            }
        }
    }

    /// <summary>
    /// Checks whether a number's text is also a JSON number, as <c>IsValidJsonNumber</c> does.
    /// </summary>
    private static bool IsValidJsonNumber(string str)
    {
        int i = 0;
        if (i < str.Length && str[i] == '-')
        {
            i++;
        }

        // Part (1): leading digits, with no superfluous leading zeros.
        if (i < str.Length && str[i] == '0')
        {
            i++;
        }
        else if (i < str.Length && str[i] >= '1' && str[i] <= '9')
        {
            while (i < str.Length && char.IsAsciiDigit(str[i]))
            {
                i++;
            }
        }
        else
        {
            return false;
        }

        // Part (2): optional fraction.
        if (i < str.Length && str[i] == '.')
        {
            i++;
            int start = i;
            while (i < str.Length && char.IsAsciiDigit(str[i]))
            {
                i++;
            }

            if (i == start)
            {
                return false;
            }
        }

        // Part (3): optional exponent.
        if (i < str.Length && (str[i] == 'e' || str[i] == 'E'))
        {
            i++;
            if (i < str.Length && (str[i] == '+' || str[i] == '-'))
            {
                i++;
            }

            int start = i;
            while (i < str.Length && char.IsAsciiDigit(str[i]))
            {
                i++;
            }

            if (i == start)
            {
                return false;
            }
        }

        return i == str.Length;
    }

    private static void AppendText(StringInfoData* result, ulong text)
    {
        sbyte* str = NativeMethods.text_to_cstring((varlena*)text);
        NativeMethods.appendStringInfoString(result, str);
        NativeMethods.pfree(str);
    }

    private static void EscapeJson(StringInfoData* @out, string value)
    {
        fixed (byte* cvalue = Worf.ToServerCString(value))
        {
            NativeMethods.escape_json(@out, (sbyte*)cvalue);
        }
    }

    private static void Append(StringInfoData* @out, string value)
    {
        byte[] bytes = Worf.ToServer(value);
        fixed (byte* data = bytes)
        {
            Worf.AppendBinaryStringInfo(@out, data, bytes.Length);
        }
    }

    private static long CommitTime(ReorderBufferTXN* txn)
    {
#if ANKUS_PG13 || ANKUS_PG14 || ANKUS_PG19
        return txn->commit_time;
#else
        return txn->xact_time.commit_time;
#endif
    }
}
