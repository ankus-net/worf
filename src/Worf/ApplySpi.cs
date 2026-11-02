using System.Text;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// worf apply functions using SPI.
/// </summary>
internal static unsafe class ApplySpi
{
    /// <summary>
    /// Handle begin.
    /// </summary>
    internal static void ApplySpiBegin()
    {
    }

    /// <summary>
    /// Handle commit.
    /// </summary>
    internal static void ApplySpiCommit()
    {
    }

    /// <summary>
    /// Handle insert via SPI.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="newtup">The new tuple.</param>
    internal static void ApplySpiInsert(WorfRelation rel, WorfTupleData newtup)
    {
        TupleDescData* desc = Desc(rel);
        StringBuilder cmd = new();
        List<SpiParameter> args = [];
        int narg = 0;

        newtup.ConvertValues(desc);

        cmd.Append("INSERT INTO ").Append(Spi.QuoteQualifiedIdentifier(rel.Nspname, rel.Relname)).Append(" (");

        for (int att = 0; att < desc->natts; att++)
        {
            FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(desc, att);
            if (attr->attisdropped)
            {
                continue;
            }

            if (!newtup.Changed[att])
            {
                continue;
            }

            cmd.Append(narg > 0 ? ", " : "").Append(Spi.QuoteIdentifier(AttName(attr)));
            narg++;
        }

        cmd.Append(") VALUES (");

        narg = 0;
        for (int att = 0; att < desc->natts; att++)
        {
            FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(desc, att);
            if (attr->attisdropped)
            {
                continue;
            }

            if (!newtup.Changed[att])
            {
                continue;
            }

            cmd.Append(narg > 0 ? ", " : "").Append(Spi.ParameterReference(narg + 1));
            args.Add(Arg(attr, newtup, att));
            narg++;
        }

        cmd.Append(')');

        ExecuteComposed(cmd.ToString(), args);
    }

    /// <summary>
    /// Handle update via SPI.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="oldtup">The old key.</param>
    /// <param name="newtup">The new tuple.</param>
    internal static void ApplySpiUpdate(WorfRelation rel, WorfTupleData oldtup, WorfTupleData newtup)
    {
        RelationData* relation = rel.Rel;
        TupleDescData* desc = relation->rd_att;
        StringBuilder cmd = new();
        List<SpiParameter> args = [];
        int att;
        int narg;

        oldtup.ConvertValues(desc);
        newtup.ConvertValues(desc);

        Bitmapset* idAttrs = NativeMethods.RelationGetIndexAttrBitmap(relation, IndexAttrBitmapKind.INDEX_ATTR_BITMAP_IDENTITY_KEY);
        Dictionary<short, string> operators = KeyOperators(relation);

        cmd.Append("UPDATE ").Append(Spi.QuoteQualifiedIdentifier(rel.Nspname, rel.Relname)).Append(" SET ");

        for (att = 0, narg = 0; att < desc->natts; att++)
        {
            FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(desc, att);
            if (attr->attisdropped)
            {
                continue;
            }

            if (!newtup.Changed[att])
            {
                continue;
            }

            cmd.Append(narg > 0 ? ", " : "").Append(Spi.QuoteIdentifier(AttName(attr))).Append(" = ")
                .Append(Spi.ParameterReference(narg + 1));
            args.Add(Arg(attr, newtup, att));
            narg++;
        }

        cmd.Append(" WHERE");

        int firstarg = narg;
        for (att = 0; att < desc->natts; att++)
        {
            FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(desc, att);
            if (!NativeMethods.bms_is_member(attr->attnum - ReplicationSet.FirstLowInvalidHeapAttributeNumber, idAttrs))
            {
                continue;
            }

            cmd.Append(narg > firstarg ? " AND " : " ").Append(Spi.QuoteIdentifier(AttName(attr))).Append(' ')
                .Append(KeyOperator(operators, attr->attnum)).Append(' ').Append(Spi.ParameterReference(narg + 1));
            args.Add(Arg(attr, oldtup, att));
            narg++;
        }

        ExecuteComposed(cmd.ToString(), args);
    }

    /// <summary>
    /// Handle delete via SPI.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="oldtup">The old key.</param>
    internal static void ApplySpiDelete(WorfRelation rel, WorfTupleData oldtup)
    {
        RelationData* relation = rel.Rel;
        TupleDescData* desc = relation->rd_att;
        StringBuilder cmd = new();
        List<SpiParameter> args = [];
        int narg = 0;

        oldtup.ConvertValues(desc);

        Bitmapset* idAttrs = NativeMethods.RelationGetIndexAttrBitmap(relation, IndexAttrBitmapKind.INDEX_ATTR_BITMAP_IDENTITY_KEY);
        Dictionary<short, string> operators = KeyOperators(relation);

        cmd.Append("DELETE FROM ").Append(Spi.QuoteQualifiedIdentifier(rel.Nspname, rel.Relname)).Append(" WHERE");

        for (int att = 0; att < desc->natts; att++)
        {
            FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(desc, att);
            if (!NativeMethods.bms_is_member(attr->attnum - ReplicationSet.FirstLowInvalidHeapAttributeNumber, idAttrs))
            {
                continue;
            }

            cmd.Append(narg > 0 ? " AND " : " ").Append(Spi.QuoteIdentifier(AttName(attr))).Append(' ')
                .Append(KeyOperator(operators, attr->attnum)).Append(' ').Append(Spi.ParameterReference(narg + 1));
            args.Add(Arg(attr, oldtup, att));
            narg++;
        }

        ExecuteComposed(cmd.ToString(), args);
    }

    /// <summary>
    /// Runs a statement composed of quoted names, parameter references and the key's operators, which come from the
    /// catalog, under the session's search path, for the triggers it fires.
    /// </summary>
    private static void ExecuteComposed(string sql, List<SpiParameter> args) => _ = Spi.Execute(sql, [.. args]);

    /// <summary>
    /// Gets the equality operators of a table's replica identity index, which the statements name with their schemas, as
    /// they run under the session's search path, for the triggers they fire.
    /// </summary>
    private static Dictionary<short, string> KeyOperators(RelationData* relation)
    {
        uint index = NativeMethods.RelationGetReplicaIndex(relation);
        return index == 0 ? [] : Worf.KeyEqualityOperators(index);
    }

    /// <summary>
    /// Gets the equality operator a key column's index compares it with.
    /// </summary>
    private static string KeyOperator(Dictionary<short, string> operators, short attnum)
        => operators.TryGetValue(attnum, out string? op) ? op : "OPERATOR(pg_catalog.=)";

    /// <summary>
    /// Whether the relation's inserts may be batched. They can't through SPI, as on Windows.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <returns>False.</returns>
    internal static bool ApplySpiCanMi(WorfRelation rel)
    {
        _ = rel;
        return false;
    }

    /// <summary>
    /// Add tuple to the MultiInsert, which is never used through SPI.
    /// </summary>
    /// <param name="rel">The relation.</param>
    /// <param name="tup">The tuple.</param>
    internal static void ApplySpiMiAddTuple(WorfRelation rel, WorfTupleData tup)
    {
        _ = rel;
        _ = tup;
        throw new PgException(PgSqlStates.InternalError, "worf.use_spi can't be used with multi insert");
    }

    /// <summary>
    /// Finish the MultiInsert, which is never used through SPI.
    /// </summary>
    /// <param name="rel">The relation.</param>
    internal static void ApplySpiMiFinish(WorfRelation rel) => _ = rel;

    private static SpiParameter Arg(FormData_pg_attribute* attr, WorfTupleData tuple, int att)
        => SpiParameter.Create(PgDatum.DangerousCreate((nuint)tuple.Values[att], attr->atttypid, PgMemoryContext.Current,
            tuple.Nulls[att]));

    private static string AttName(FormData_pg_attribute* attr) => Worf.FromServer((sbyte*)&attr->attname);

    private static TupleDescData* Desc(WorfRelation rel) => (rel.Rel)->rd_att;
}
