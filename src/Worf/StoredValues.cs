using System.Globalization;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Reads the values a table's rows hold in an integer or numeric column with PostgreSQL's own scans: through a valid
/// btree index that the column leads, without a predicate, or else through the table's heap. The scans run none of the
/// user's functions, see every row whatever the table's row-level security, and need no rights on the table or its
/// schema. A heap scan for values in a span is spared where the table's validated CHECK constraints, or its partition
/// bound under a partition key of plain columns, rule the span out, which they show as stored, without running anything.
/// </summary>
internal static unsafe class StoredValues
{
    private const uint Int2Oid = 21;
    private const uint Int4Oid = 23;
    private const uint Int8Oid = 20;
    private const uint NumericOid = 1700;
    private const uint BoolOid = 16;

    // integer_ops and numeric_ops, btree's operator families for the column's values, and the functions that read and
    // write a numeric value as text.
    private const uint IntegerOps = 1976;
    private const uint NumericOps = 1988;
    private const uint NumericIn = 1701;
    private const uint NumericOut = 1702;

    // BTLessEqualStrategyNumber and BTGreaterEqualStrategyNumber from access/stratnum.h.
    private const short LessEqual = 2;
    private const short GreaterEqual = 4;

    /// <summary>
    /// Finds a value a table's rows hold in a column in any of a set of spans: a whole number, for a numeric column.
    /// </summary>
    /// <param name="relid">The table.</param>
    /// <param name="inherited">Whether to read its inheritors' and partitions' rows too.</param>
    /// <param name="column">The column.</param>
    /// <param name="spans">The spans.</param>
    /// <returns>The value as text, or null when no row holds one.</returns>
    internal static string? FindWithin(uint relid, bool inherited, string column, List<(long Lo, long Hi)> spans)
        => WithSnapshot(() =>
        {
            foreach (uint table in Tables(relid, inherited))
            {
                string? found = null;
                Read(table, column, (rel, attnum, typeOid) =>
                {
                    foreach ((long lo, long hi) in spans)
                    {
                        if (!Scan(rel, attnum, typeOid, lo, hi, false, value => (found = Whole(value, typeOid)) is not null) &&
                            !Refuted(rel, attnum, typeOid, lo, hi))
                        {
                            _ = Heap(rel, attnum, typeOid, lo, hi, value => (found = Whole(value, typeOid)) is not null);
                        }

                        if (found is not null)
                        {
                            return;
                        }
                    }
                });
                if (found is not null)
                {
                    return found;
                }
            }

            return null;
        });

    /// <summary>
    /// Finds the furthest value a table's rows, and its inheritors' and partitions', hold in a column within a span: the
    /// greatest for an ascending sequence, the least for a descending one.
    /// </summary>
    /// <param name="relid">The table.</param>
    /// <param name="column">The column.</param>
    /// <param name="ascending">Whether the sequence ascends.</param>
    /// <param name="lo">The span's start.</param>
    /// <param name="hi">The span's end.</param>
    /// <param name="indexOnly">Whether to read only tables with an index on the column, and only the table itself.</param>
    /// <returns>The value, or null when no row holds one.</returns>
    internal static decimal? Furthest(uint relid, string column, bool ascending, long lo, long hi, bool indexOnly)
        => WithSnapshot(() =>
        {
            decimal? furthest = null;
            void Take(decimal value) => furthest = furthest is not decimal f ? value : ascending ? Math.Max(f, value) : Math.Min(f, value);
            foreach (uint table in Tables(relid, !indexOnly))
            {
                Read(table, column, (rel, attnum, typeOid) =>
                {
                    // An index gives the furthest value first, while the heap gives every value.
                    if (!Scan(rel, attnum, typeOid, lo, hi, ascending, value =>
                        {
                            Take(Value(value, typeOid));
                            return true;
                        }) && !indexOnly)
                    {
                        _ = Heap(rel, attnum, typeOid, lo, hi, value =>
                        {
                            Take(Value(value, typeOid));
                            return false;
                        });
                    }
                });
            }

            return furthest;
        });

    /// <summary>
    /// Lists a table, with its inheritors and partitions when asked, that have rows of their own.
    /// </summary>
    private static List<uint> Tables(uint relid, bool inherited)
    {
        List<uint> tables = [];
        List* members = inherited ? NativeMethods.find_all_inheritors(relid, (int)PgLockMode.AccessShare, null) : null;
        for (int i = 0; i < (inherited ? members->length : 1); i++)
        {
            uint table = inherited ? members->elements[i].oid_value : relid;
            if (NativeMethods.get_rel_relkind(table) == (sbyte)'r')
            {
                tables.Add(table);
            }
        }

        return tables;
    }

    /// <summary>
    /// Opens a table, under a lock that only writes to its structure wait for, and reads a column of it, as long as the
    /// table has the column, of an integer or numeric type.
    /// </summary>
    private static void Read(uint relid, string column, Action<nint, short, uint> read)
    {
        short attnum = NativeMethods.get_attnum(relid, Worf.PStrDup(column));
        if (attnum <= 0)
        {
            return;
        }

        uint typeOid = NativeMethods.getBaseType(NativeMethods.get_atttype(relid, attnum));
        if (typeOid is not (Int2Oid or Int4Oid or Int8Oid or NumericOid))
        {
            return;
        }

        RelationData* rel = NativeMethods.table_open(relid, (int)PgLockMode.AccessShare);
        try
        {
            read((nint)rel, attnum, typeOid);
        }
        finally
        {
            NativeMethods.table_close(rel, (int)PgLockMode.None);
        }
    }

    /// <summary>
    /// Reads the values in a span through a btree index that the column leads, if the table has one, in order, the
    /// greatest first when asked, until <paramref name="stop"/> says to stop.
    /// </summary>
    /// <returns>Whether the table has such an index.</returns>
    private static bool Scan(nint relation, short attnum, uint typeOid, long lo, long hi, bool greatestFirst, Func<ulong, bool> stop)
    {
        RelationData* rel = (RelationData*)relation;
        uint family = typeOid == NumericOid ? NumericOps : IntegerOps;
        WorfRows indexes = Worf.Read("""
            SELECT i.indexrelid, (i.indoption[0] & 1) <> 0
            FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
                JOIN pg_catalog.pg_am m ON m.oid = c.relam
                JOIN pg_catalog.pg_opclass o ON o.oid = i.indclass[0]
            WHERE i.indrelid = $1 AND i.indkey[0] = $2 AND m.amname = 'btree' AND o.opcfamily = $3
                AND i.indisvalid AND i.indisready AND i.indpred IS NULL
            ORDER BY i.indexrelid
            LIMIT 1
            """, 1, SpiParameter.Create(rel->rd_id), SpiParameter.Create(attnum), SpiParameter.Create(family));
        if (indexes.Count == 0)
        {
            return false;
        }

        // An index sorted in descending order gives the greatest values first going forward.
        bool forward = greatestFirst == indexes[0].Get<bool>(1);
        RelationData* index = NativeMethods.index_open(indexes[0].Get<uint>(0), (int)PgLockMode.AccessShare);
        ScanKeyData* keys = stackalloc ScanKeyData[2];
        Keys(keys, attnum, typeOid, lo, hi);
        SysScanDescData* scan = NativeMethods.systable_beginscan_ordered(rel, index, NativeMethods.GetActiveSnapshot(), 2, keys);
        try
        {
            for (HeapTupleData* tuple; (tuple = NativeMethods.systable_getnext_ordered(scan,
                forward ? ScanDirection.ForwardScanDirection : ScanDirection.BackwardScanDirection)) is not null;)
            {
                bool isnull;
                ulong value = NativeMethods.heap_getattr(tuple, attnum, rel->rd_att, &isnull);
                if (!isnull && stop(value))
                {
                    break;
                }
            }
        }
        finally
        {
            NativeMethods.systable_endscan_ordered(scan);
            NativeMethods.index_close(index, (int)PgLockMode.None);
        }

        return true;
    }

    /// <summary>
    /// Reads the values in a span through the table's heap, until <paramref name="stop"/> says to stop.
    /// </summary>
    /// <returns>Whether it stopped.</returns>
    private static bool Heap(nint relation, short attnum, uint typeOid, long lo, long hi, Func<ulong, bool> stop)
    {
        RelationData* rel = (RelationData*)relation;
        ScanKeyData* keys = stackalloc ScanKeyData[2];
        Keys(keys, attnum, typeOid, lo, hi);
        SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, false, NativeMethods.GetActiveSnapshot(), 2, keys);
        try
        {
            for (HeapTupleData* tuple; (tuple = NativeMethods.systable_getnext(scan)) is not null;)
            {
                bool isnull;
                ulong value = NativeMethods.heap_getattr(tuple, attnum, rel->rd_att, &isnull);
                if (!isnull && stop(value))
                {
                    return true;
                }
            }
        }
        finally
        {
            NativeMethods.systable_endscan(scan);
        }

        return false;
    }

    /// <summary>
    /// Sets the scan keys for a span: a column's value at least its start and at most its end, compared as btree
    /// compares them, against bigint values for an integer column.
    /// </summary>
    private static void Keys(ScanKeyData* keys, short attnum, uint typeOid, long lo, long hi)
    {
        uint family = typeOid == NumericOid ? NumericOps : IntegerOps;
        uint bound = typeOid == NumericOid ? NumericOid : Int8Oid;
        foreach ((int i, short strategy, long value) in new[] { (0, GreaterEqual, lo), (1, LessEqual, hi) })
        {
            uint op = NativeMethods.get_opfamily_member(family, typeOid, bound, strategy);
            NativeMethods.ScanKeyEntryInitialize(&keys[i], 0, attnum, (ushort)strategy, bound, 0, NativeMethods.get_opcode(op), Bound(typeOid, value));
        }
    }

    /// <summary>
    /// Makes the datum of a span's end, a bigint, or a numeric for a numeric column.
    /// </summary>
    private static ulong Bound(uint typeOid, long value)
        => typeOid == NumericOid
            ? NativeMethods.OidInputFunctionCall(NumericIn, Worf.PStrDup(value.ToString(CultureInfo.InvariantCulture)), NumericOid, -1)
            : (ulong)value;

    /// <summary>
    /// Checks whether the table's validated CHECK constraints, or its partition bound under a partition key of plain
    /// columns, which builds none of the user's expressions, rule a span out.
    /// </summary>
    private static bool Refuted(nint relation, short attnum, uint typeOid, long lo, long hi)
    {
        RelationData* rel = (RelationData*)relation;

        // A domain's column compares as its base type, which its constraints show as a cast this check doesn't follow.
        if (NativeMethods.get_atttype(rel->rd_id, attnum) != typeOid)
        {
            return false;
        }

        WorfRows checks = Worf.Read("""
            SELECT conbin::text FROM pg_catalog.pg_constraint WHERE conrelid = $1 AND contype = 'c' AND convalidated
            """, 0, SpiParameter.Create(rel->rd_id));
        List* constraints = null;
        for (int i = 0; i < checks.Count; i++)
        {
            constraints = NativeMethods.list_concat(constraints,
                NativeMethods.make_ands_implicit((Expr*)NativeMethods.stringToNode(Worf.PStrDup(checks[i].Get<string>(0)))));
        }

        if (constraints is not null && NativeMethods.predicate_refuted_by(constraints, Clause(attnum, typeOid, lo, hi), false))
        {
            return true;
        }

        // A partition's bound, made from its parent's key, of the parent's columns.
        WorfRows bound = Worf.Read("""
            SELECT c.relpartbound::text, i.inhparent
            FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_inherits i ON i.inhrelid = c.oid
                JOIN pg_catalog.pg_partitioned_table p ON p.partrelid = i.inhparent
            WHERE c.oid = $1 AND c.relispartition AND p.partexprs IS NULL
            """, 1, SpiParameter.Create(rel->rd_id));
        if (bound.Count == 0)
        {
            return false;
        }

        uint parentId = bound[0].Get<uint>(1);
        short parentAttnum = NativeMethods.get_attnum(parentId, NativeMethods.get_attname(rel->rd_id, attnum, false));
        RelationData* parent = NativeMethods.table_open(parentId, (int)PgLockMode.AccessShare);
        try
        {
            List* qual = NativeMethods.get_qual_from_partbound(parent,
                (PartitionBoundSpec*)NativeMethods.stringToNode(Worf.PStrDup(bound[0].Get<string>(0))));
            return parentAttnum > 0 && qual is not null && NativeMethods.predicate_refuted_by(qual, Clause(parentAttnum, typeOid, lo, hi), false);
        }
        finally
        {
            NativeMethods.table_close(parent, (int)PgLockMode.None);
        }
    }

    /// <summary>
    /// Makes the condition that a column's value lies in a span, as a scan's keys test it.
    /// </summary>
    private static List* Clause(short attnum, uint typeOid, long lo, long hi)
    {
        uint family = typeOid == NumericOid ? NumericOps : IntegerOps;
        uint bound = typeOid == NumericOid ? NumericOid : Int8Oid;
        List* clause = null;
        foreach ((short strategy, long value) in new[] { (GreaterEqual, lo), (LessEqual, hi) })
        {
            Var* column = NativeMethods.makeVar(1, attnum, typeOid, -1, 0, 0);
            Const* constant = NativeMethods.makeConst(bound, -1, 0, typeOid == NumericOid ? -1 : 8, Bound(typeOid, value), false, typeOid != NumericOid);
            clause = NativeMethods.lappend(clause, NativeMethods.make_opclause(NativeMethods.get_opfamily_member(family, typeOid, bound, strategy), BoolOid,
                false, (Expr*)column, (Expr*)constant, 0, 0));
        }

        return clause;
    }

    /// <summary>
    /// Reads a column's value as a number.
    /// </summary>
    private static decimal Value(ulong datum, uint typeOid) => typeOid switch
    {
        Int2Oid => (short)datum,
        Int4Oid => (int)datum,
        Int8Oid => (long)datum,
        _ => decimal.Parse(Worf.FromServer(NativeMethods.OidOutputFunctionCall(NumericOut, datum)), NumberStyles.Number, CultureInfo.InvariantCulture),
    };

    /// <summary>
    /// Reads a column's value as text, when it is a whole number.
    /// </summary>
    private static string? Whole(ulong datum, uint typeOid)
    {
        decimal value = Value(datum, typeOid);
        return decimal.Truncate(value) == value ? value.ToString(CultureInfo.InvariantCulture) : null;
    }

    /// <summary>
    /// Runs reads with the snapshot a statement of the transaction takes, as <see cref="Worf.Read"/> does.
    /// </summary>
    private static T WithSnapshot<T>(Func<T> read)
    {
        NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());
        try
        {
            return read();
        }
        finally
        {
            NativeMethods.PopActiveSnapshot();
        }
    }
}
