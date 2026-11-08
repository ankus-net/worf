using System.Globalization;

namespace Worf;

/// <summary>
/// Finds the rows of a table's copy that the table's immediate unique indexes and exclusion constraints would refuse,
/// before they're written, all at once rather than a transaction ID each, as COPY or a MERGE writing them would fail
/// them one at a time: a row whose keys a row the table keeps holds, or an earlier row of the copy that's written
/// takes, in any of them, in the copy's order, as PostgreSQL checks the indexes of each row it writes in turn, so that a
/// row left out for one index doesn't hold its keys in another. Indexes on expressions and with predicates are compared
/// as PostgreSQL compares them, and exclusion constraints by their operators. A table with a BEFORE ROW trigger that
/// fires for the copy, which may change a row before it's checked, isn't checked.
/// </summary>
internal static class HeldValues
{
    /// <summary>
    /// Finds a table's immediate unique indexes and exclusion constraints whose columns the copy writes, in the order
    /// PostgreSQL checks them, as their indexes' OIDs go.
    /// </summary>
    /// <param name="reloid">The table.</param>
    /// <param name="columns">The columns the copy writes.</param>
    /// <param name="leftOut">An index left out, as the key a merge matches rows by, or zero.</param>
    /// <returns>The constraints, or none when a BEFORE ROW trigger may change the rows first.</returns>
    internal static List<HeldConstraint> Find(uint reloid, IReadOnlyCollection<string> columns, uint leftOut)
    {
        if (ChangedBeforeWritten(reloid))
        {
            return [];
        }

        WorfRows indexes = Worf.Read("""
            SELECT k.name, k.exclusion, pg_catalog.array_agg(k.key ORDER BY k.n), pg_catalog.array_agg(k.collation ORDER BY k.n),
                pg_catalog.array_agg(k.op ORDER BY k.n), pg_catalog.array_agg(k.ordering ORDER BY k.n), k.predicate, k.nullsequal, k.amname,
                pg_catalog.array_agg(k.opclass ORDER BY k.n), k.reads
            FROM (
                SELECT i.indexrelid, i.indnkeyatts, x.relname::pg_catalog.text AS name, c.oid IS NOT NULL AS exclusion, k.n,
                    pg_catalog.pg_get_indexdef(i.indexrelid, k.n::pg_catalog.int4, false) AS key,
                    CASE WHEN co.oid IS NULL THEN ''
                        ELSE ' COLLATE ' || pg_catalog.quote_ident(cn.nspname) || '.' || pg_catalog.quote_ident(co.collname) END AS collation,
                    pg_catalog.format('OPERATOR(%I.%s)', opn.nspname, op.oprname) AS op, lt.op AS ordering,
                    pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS predicate, i.indnullsnotdistinct AS nullsequal, am.amname::pg_catalog.text AS amname,
                    pg_catalog.quote_ident(ocn.nspname) || '.' || pg_catalog.quote_ident(oc.opcname) AS opclass,
                    (SELECT pg_catalog.array_agg(a.attname::pg_catalog.text ORDER BY a.attnum) FROM pg_catalog.pg_attribute a
                        WHERE a.attrelid = i.indrelid AND a.attnum > 0
                            AND (a.attnum = ANY ((i.indkey::pg_catalog.int2[])[0:i.indnkeyatts - 1])
                                OR a.attnum IN (SELECT d.refobjsubid FROM pg_catalog.pg_depend d
                                    WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = i.indexrelid
                                        AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjid = i.indrelid))) AS reads
                FROM pg_catalog.pg_index i
                    JOIN pg_catalog.pg_class x ON x.oid = i.indexrelid
                    JOIN pg_catalog.pg_am am ON am.oid = x.relam
                    LEFT JOIN pg_catalog.pg_constraint c ON c.conindid = i.indexrelid AND c.conrelid = i.indrelid AND c.contype = 'x'
                    CROSS JOIN LATERAL ROWS FROM (pg_catalog.unnest(i.indclass::pg_catalog.oid[]), pg_catalog.unnest(i.indcollation::pg_catalog.oid[]))
                        WITH ORDINALITY AS k (opc, coll, n)
                    JOIN pg_catalog.pg_opclass oc ON oc.oid = k.opc
                    JOIN pg_catalog.pg_namespace ocn ON ocn.oid = oc.opcnamespace
                    LEFT JOIN pg_catalog.pg_collation co ON co.oid = k.coll
                    LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace
                    JOIN pg_catalog.pg_operator op ON op.oid = CASE WHEN c.oid IS NULL
                        THEN (SELECT e.amopopr FROM pg_catalog.pg_amop e WHERE e.amopfamily = oc.opcfamily AND e.amoplefttype = oc.opcintype
                            AND e.amoprighttype = oc.opcintype AND e.amopstrategy = 3)
                        ELSE c.conexclop[k.n::pg_catalog.int4] END
                    JOIN pg_catalog.pg_namespace opn ON opn.oid = op.oprnamespace
                    LEFT JOIN LATERAL (
                        SELECT pg_catalog.format('OPERATOR(%I.%s)', ln.nspname, lo.oprname) AS op
                        FROM pg_catalog.pg_amop q
                            JOIN pg_catalog.pg_amop l ON l.amopfamily = q.amopfamily AND l.amoplefttype = q.amoplefttype
                                AND l.amoprighttype = q.amoprighttype AND l.amopstrategy = 1
                            JOIN pg_catalog.pg_operator lo ON lo.oid = l.amopopr
                            JOIN pg_catalog.pg_namespace ln ON ln.oid = lo.oprnamespace
                        WHERE q.amopopr = op.oid AND q.amopmethod = 403 AND q.amopstrategy = 3
                        LIMIT 1) lt ON true
                WHERE i.indrelid = $1 AND i.indexrelid <> $2 AND i.indisvalid AND i.indislive AND i.indimmediate
                    AND (i.indisunique OR c.oid IS NOT NULL) AND k.n <= i.indnkeyatts) k
            GROUP BY k.indexrelid, k.indnkeyatts, k.name, k.exclusion, k.predicate, k.nullsequal, k.amname, k.reads
            HAVING pg_catalog.count(*) = k.indnkeyatts
            ORDER BY k.indexrelid
            """, 0, SpiParameter.Create(reloid), SpiParameter.Create(leftOut));
        List<HeldConstraint> found = [];
        foreach (WorfRow index in indexes)
        {
            // An index whose keys read no column, as one on a whole row, isn't checked, nor one that reads a column the
            // copy doesn't write, whose value comes from its default.
            if (index.Get<string[]?>(10) is not string[] read || !read.All(columns.Contains))
            {
                continue;
            }

            string?[] orderings = index.Get<string?[]>(5);
            found.Add(new HeldConstraint(index.Get<string>(0), index.Get<bool>(1), index.Get<string[]>(2), index.Get<string[]>(3),
                index.Get<string[]>(4), Array.TrueForAll(orderings, ordering => ordering is not null) ? [.. orderings.Select(ordering => ordering!)] : null,
                index.Get<string?>(6), index.Get<bool>(7), index.Get<string>(8), index.Get<string[]>(9), read));
        }

        return found;
    }

    /// <summary>
    /// Finds the rows of a copy that the constraints refuse, in the copy's order: each with the error of the first of
    /// the constraints that refuses it, as PostgreSQL would raise it. A check that fails, as on a value an index's
    /// expression fails on, is left out, for the write to fail on.
    /// </summary>
    /// <param name="constraints">The constraints, in the order PostgreSQL checks them.</param>
    /// <param name="table">The table, qualified, after ONLY when it isn't partitioned.</param>
    /// <param name="candidates">A relation of the copy's rows that are written, whose columns are named as the
    /// table's, with each row's place in the copy in <paramref name="row"/>.</param>
    /// <param name="row">The column of each candidate's place in the copy, a bigint.</param>
    /// <param name="kept">The condition, on a row of the table as <c>t</c>, that it stays as it is.</param>
    /// <param name="indexed">Whether the candidates are many enough that those an exclusion constraint compares with
    /// each other other than by equality are kept in a table with an index for the comparison.</param>
    /// <param name="parameters">The parameters the candidates' relation reads.</param>
    /// <returns>The rows refused, by their places, with their errors.</returns>
    internal static SortedDictionary<long, PgException> Refused(List<HeldConstraint> constraints, string table, string candidates, string row,
        string kept, bool indexed, params SpiParameter[] parameters)
    {
        RefusedChecked = 0;
        // Each candidate's conflicts, by constraint: with a row the table keeps, with the other row's keys, and the rows
        // whose keys are the same as its own, numbered as one, or the earlier rows its keys conflict with.
        SortedDictionary<long, List<(int Constraint, string Key, string? Kept, long Same, List<(long Row, string Key)>? Earlier)>> conflicts = [];
        for (int c = 0; c < constraints.Count; c++)
        {
            HeldConstraint constraint = constraints[c];
            List<string> conditions = [.. constraint.Predicate is string predicate ? [$"({predicate})"] : Array.Empty<string>()];
            if (!constraint.NullsEqual)
            {
                conditions.AddRange(constraint.Compared.Select(key => $"{key} IS NOT NULL"));
            }

            string where = conditions.Count == 0 ? "" : $" WHERE {string.Join(" AND ", conditions)}";
            string keys = $"SELECT s.{row} AS r, {string.Join(", ", constraint.Compared.Select((key, k) => $"{key} AS k{k}"))} FROM {candidates} s{where}";
            string keyText = KeyText(constraint.Keys.Length, "b.k");
            string inTable = $"""
                (SELECT {KeyText(constraint.Keys.Length, null, constraint.Compared)} FROM {table} t
                WHERE {(constraint.Predicate is string tablePredicate ? $"({tablePredicate}) AND " : "")}{constraint.Conflict(null, "b")} AND {kept} LIMIT 1)
                """;
            int found = c;
            try
            {
                PgTransaction.RunInSubtransaction(() =>
                {
                    if (constraint.Orderings is string[] orderings)
                    {
                        // Keys compared by equality conflict when they're the same: the rows with the same keys are
                        // numbered as one, in the order of the btree operator class the equality comes from.
                        // Only the candidates that can conflict come back, those a row the table keeps holds the keys of
                        // and those that share their keys, so that what's kept grows with the conflicts, not the candidates.
                        foreach (WorfRow match in Worf.Read($"""
                            WITH b AS MATERIALIZED ({keys})
                            SELECT y.r, y.key, y.kept, y.same FROM (
                                SELECT x.*, pg_catalog.count(*) OVER (PARTITION BY x.same) AS n FROM (
                                    SELECT b.r, {keyText} AS key, {inTable} AS kept,
                                        pg_catalog.dense_rank() OVER (ORDER BY {string.Join(", ", orderings.Select((ordering, k) => $"b.k{k} USING {ordering}"))}) AS same
                                    FROM b) x) y
                            WHERE y.kept IS NOT NULL OR y.n > 1
                            """, 0, parameters))
                        {
                            Add(match.Get<long>(0), (found, match.Get<string>(1), match.Get<string?>(2), match.Get<long>(3), null));
                        }

                        return;
                    }

                    // Other keys are compared pair by pair, those of many candidates through an index of the constraint's
                    // own access method and operator classes.
                    string from = "b";
                    string with = $"WITH b AS MATERIALIZED ({keys}) ";
                    if (indexed)
                    {
                        from = "worf_held_keys";
                        with = "";
                        _ = Worf.Execute($"CREATE TEMPORARY TABLE worf_held_keys ON COMMIT DROP AS {keys}", parameters);
                        _ = Worf.Execute($"CREATE INDEX ON worf_held_keys USING {Worf.QuoteIdentifier(constraint.AccessMethod)} " +
                            $"({string.Join(", ", constraint.OperatorClasses.Select((opclass, k) => $"k{k} {opclass}"))})");
                        _ = Worf.Execute("ANALYZE worf_held_keys");
                    }

                    foreach (WorfRow match in Worf.Read($"""
                        {with}SELECT x.* FROM (SELECT b.r, {keyText} AS key, {inTable} AS kept, NULL::pg_catalog.int8 AS earlier, NULL::pg_catalog.text AS earlier_key FROM {from} b) x
                        WHERE x.kept IS NOT NULL
                        UNION ALL
                        SELECT b.r, {keyText}, NULL, a.r, {KeyText(constraint.Keys.Length, "a.k")}
                        FROM {from} a JOIN {from} b ON a.r OPERATOR(pg_catalog.<) b.r AND {constraint.Conflict("a", "b")}
                        """, 0, indexed ? [] : parameters))
                    {
                        long candidate = match.Get<long>(0);
                        if (match.Get<long?>(3) is long earlier)
                        {
                            Add(candidate, (found, match.Get<string>(1), null, 0, [(earlier, match.Get<string>(4))]));
                        }
                        else if (match.Get<string?>(2) is string other)
                        {
                            Add(candidate, (found, match.Get<string>(1), other, 0, []));
                        }
                    }

                    if (indexed)
                    {
                        _ = Worf.Execute("DROP TABLE worf_held_keys");
                        Worf.EndForDecoding();
                    }
                }, PgSubtransactionMode.Atomic);
            }
            catch (PgException error) when (!Apply.IsTransient(error))
            {
                foreach (List<(int Constraint, string, string?, long, List<(long, string)>?)> rowConflicts in conflicts.Values)
                {
                    _ = rowConflicts.RemoveAll(conflict => conflict.Constraint == found);
                }
            }
        }

        // In the copy's order, a row is refused by the first constraint in which a row the table keeps holds its keys,
        // or an earlier row that's written takes them; a row written takes its keys in every constraint.
        RefusedChecked = conflicts.Count;
        SortedDictionary<long, PgException> refused = [];
        Dictionary<(int Constraint, long Same), string> taken = [];
        foreach ((long candidate, List<(int Constraint, string Key, string? Kept, long Same, List<(long Row, string Key)>? Earlier)> rowConflicts) in conflicts)
        {
            foreach (IGrouping<int, (int Constraint, string Key, string? Kept, long Same, List<(long Row, string Key)>? Earlier)> byConstraint in
                rowConflicts.GroupBy(conflict => conflict.Constraint).OrderBy(group => group.Key))
            {
                HeldConstraint constraint = constraints[byConstraint.Key];
                (int _, string key, string? _, long same, List<(long Row, string Key)>? earlier) = byConstraint.First();
                string? other = byConstraint.Select(conflict => conflict.Kept).FirstOrDefault(kept => kept is not null)
                    ?? (earlier is null
                        ? taken.GetValueOrDefault((byConstraint.Key, same))
                        : byConstraint.SelectMany(conflict => conflict.Earlier!).Where(pair => !refused.ContainsKey(pair.Row)).Select(pair => pair.Key).FirstOrDefault());
                if (other is not null)
                {
                    refused[candidate] = constraint.Error(key, other);
                    break;
                }
            }

            if (!refused.ContainsKey(candidate))
            {
                foreach ((int c, string key, string? _, long same, List<(long, string)>? earlier) in rowConflicts.Where(conflict => conflict.Earlier is null))
                {
                    _ = taken.TryAdd((c, same), key);
                }
            }
        }

        return refused;

        void Add(long candidate, (int, string, string?, long, List<(long, string)>?) conflict)
        {
            if (!conflicts.TryGetValue(candidate, out List<(int, string, string?, long, List<(long, string)>?)>? list))
            {
                conflicts[candidate] = list = [];
            }

            list.Add(conflict);
        }
    }

    /// <summary>
    /// Gets how many candidates the last <see cref="Refused"/> kept to decide on, those that could conflict.
    /// </summary>
    internal static int RefusedChecked { get; private set; }

    /// <summary>
    /// Checks whether a table, or one of its partitions, has a BEFORE ROW INSERT or UPDATE trigger that fires under
    /// session_replication_role replica, as it fires for a copy's rows, and may change one before PostgreSQL checks it.
    /// </summary>
    /// <param name="reloid">The table.</param>
    /// <returns>Whether it has one.</returns>
    internal static bool ChangedBeforeWritten(uint reloid) => Worf.ReadScalar<bool>("""
        SELECT EXISTS (SELECT FROM pg_catalog.pg_trigger g
            WHERE (g.tgrelid = $1 OR g.tgrelid IN (SELECT p.relid FROM pg_catalog.pg_partition_tree($1) p))
                AND g.tgenabled IN ('A', 'R') AND (g.tgtype OPERATOR(pg_catalog.&) 3::pg_catalog.int2) = 3
                AND (g.tgtype OPERATOR(pg_catalog.&) 20::pg_catalog.int2) <> 0)
        """, SpiParameter.Create(reloid));

    /// <summary>
    /// Gets a name for a column that none of a table's columns has, from a stem.
    /// </summary>
    /// <param name="columns">The table's columns.</param>
    /// <param name="stem">The stem.</param>
    /// <returns>The name.</returns>
    internal static string FreeName(IReadOnlyCollection<string> columns, string stem)
    {
        string name = stem;
        for (int i = 1; columns.Contains(name); i++)
        {
            name = string.Create(CultureInfo.InvariantCulture, $"{stem}{i}");
        }

        return name;
    }

    // The text of keys as PostgreSQL shows them in an error's detail, each by its type's output function: the columns k0,
    // k1 and so on after a prefix, or the expressions given.
    private static string KeyText(int count, string? prefix, IEnumerable<string>? keys = null)
        => $"pg_catalog.concat_ws(', ', {string.Join(", ", (keys ?? Enumerable.Range(0, count).Select(k => $"{prefix}{k}")).Select(key =>
            $"CASE WHEN pg_catalog.num_nulls({key}) = 1 THEN 'null' ELSE pg_catalog.format('%s', {key}) END"))})";
}
