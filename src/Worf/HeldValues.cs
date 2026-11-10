using System.Diagnostics;
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
    /// expression fails on, is left out, for the write to fail on. The conflicts of many candidates, as a merge's, go in
    /// a temporary table rather than this backend's memory, indexed by candidate, and are decided in the copy's order a
    /// thousand candidates at a time, with the keys that rows written take, and the rows refused, in temporary tables
    /// too, read for each part, so that what's kept here doesn't grow with the copy. A row the table keeps is looked up
    /// for a constraint compared pair by pair by a join of the candidates' keys to the table, which the constraint's own
    /// index serves, rather than a search for each candidate.
    /// </summary>
    /// <param name="constraints">The constraints, in the order PostgreSQL checks them.</param>
    /// <param name="table">The table, qualified, after ONLY when it isn't partitioned.</param>
    /// <param name="candidates">A relation of the copy's rows that are written, whose columns are named as the
    /// table's, with each row's place in the copy in <paramref name="row"/>.</param>
    /// <param name="row">The column of each candidate's place in the copy, a bigint.</param>
    /// <param name="kept">The condition, on a row of the table as <c>t</c>, that it stays as it is.</param>
    /// <param name="many">Whether the candidates are many, as a merge's, so that their conflicts are kept in a temporary
    /// table, and those an exclusion constraint compares with each other other than by equality in a table with an index
    /// for the comparison.</param>
    /// <param name="refuse">Takes the rows refused, by their places, with their errors, in the copy's order, a part at
    /// a time.</param>
    /// <param name="parameters">The parameters the candidates' relation reads.</param>
    /// <returns>How many rows are refused.</returns>
    internal static long Refused(List<HeldConstraint> constraints, string table, string candidates, string row,
        string kept, bool many, Action<List<(long Row, PgException Error)>> refuse, params SpiParameter[] parameters)
    {
        RefusedChecked = 0;
        HeldMost = 0;
        long started = Stopwatch.GetTimestamp();

        // Each candidate's conflicts, by constraint: with a row the table keeps, with that row's keys, the rows whose keys
        // are the same as its own, numbered as one, or an earlier row its keys conflict with, with that row's keys.
        const string Columns = "r, c, key, kept, same, earlier, earlier_key";
        List<(long R, int C, string Key, string? Kept, long Same, long? Earlier, string? EarlierKey)> gathered = [];
        if (many)
        {
            _ = Worf.Execute("""
                CREATE TEMPORARY TABLE worf_held_conflicts (r pg_catalog.int8, c pg_catalog.int4, key pg_catalog.text, kept pg_catalog.text,
                    same pg_catalog.int8, earlier pg_catalog.int8, earlier_key pg_catalog.text);
                CREATE TEMPORARY TABLE worf_held_taken (c pg_catalog.int4, same pg_catalog.int8, key pg_catalog.text, PRIMARY KEY (c, same));
                CREATE TEMPORARY TABLE worf_held_refused (r pg_catalog.int8 PRIMARY KEY);
                """);
        }

        // The candidates' keys, joined to the table, are named apart from the table's columns, which its keys and
        // predicate read unqualified.
        HashSet<string> tableColumns = [.. Worf.Read($"""
            SELECT a.attname::pg_catalog.text FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = '{(table.StartsWith("ONLY ", StringComparison.Ordinal) ? table[5..] : table).Replace("'", "''", StringComparison.Ordinal)}'::pg_catalog.regclass
                AND a.attnum > 0
            """, 0).Select(column => column.Get<string>(0))];
        string candidateName = FreeName(tableColumns, "worf_held_r");
        string keyStem = "worf_held_k";
        for (int i = 1; constraints.Exists(constraint => Enumerable.Range(0, constraint.Keys.Length).Any(k => tableColumns.Contains($"{keyStem}{k}"))); i++)
        {
            keyStem = string.Create(CultureInfo.InvariantCulture, $"worf_held_k{i}_");
        }

        bool decided = false;
        try
        {
            for (int c = 0; c < constraints.Count; c++)
            {
                HeldConstraint constraint = constraints[c];
                List<string> conditions = [.. constraint.Predicate is string predicate ? [$"({predicate})"] : Array.Empty<string>()];
                if (!constraint.NullsEqual)
                {
                    conditions.AddRange(constraint.Compared.Select(key => $"{key} IS DISTINCT FROM NULL"));
                }

                string where = conditions.Count == 0 ? "" : $" WHERE {string.Join(" AND ", conditions)}";
                string keys = $"SELECT s.{row} AS r, {string.Join(", ", constraint.Compared.Select((key, k) => $"{key} AS k{k}"))} FROM {candidates} s{where}";
                string keyText = KeyText(constraint.Keys.Length, "b.k");
                string inTable = $"""
                    (SELECT {KeyText(constraint.Keys.Length, null, constraint.Compared)} FROM {table} t
                    WHERE {(constraint.Predicate is string tablePredicate ? $"({tablePredicate}) AND " : "")}{constraint.Conflict(null, "b")} AND {kept} LIMIT 1)
                    """;
                string number = c.ToString(CultureInfo.InvariantCulture);
                string into = many ? $"INSERT INTO pg_temp.worf_held_conflicts ({Columns}) " : "";

                // A constraint whose check fails adds none of its conflicts.
                List<(long, int, string, string?, long, long?, string?)> found = [];
                try
                {
                    PgTransaction.RunInSubtransaction(() =>
                    {
                        found.Clear();
                        string sql;
                        bool bound = true;
                        if (constraint.Orderings is string[] orderings)
                        {
                            // Keys compared by equality conflict when they're the same: the rows with the same keys are
                            // numbered as one, in the order of the btree operator class the equality comes from.
                            // Only the candidates that can conflict are kept, those a row the table keeps holds the keys
                            // of and those that share their keys.
                            sql = $"""
                                WITH b AS MATERIALIZED ({keys})
                                {into}SELECT y.r, {number}, y.key, y.kept, y.same, NULL::pg_catalog.int8, NULL::pg_catalog.text FROM (
                                    SELECT x.*, pg_catalog.count(*) OVER (PARTITION BY x.same) AS n FROM (
                                        SELECT b.r, {keyText} AS key, {inTable} AS kept,
                                            pg_catalog.dense_rank() OVER (ORDER BY {string.Join(", ", orderings.Select((ordering, k) => $"b.k{k} USING {ordering}"))}) AS same
                                        FROM b) x) y
                                WHERE y.kept IS NOT NULL OR y.n > 1
                                """;
                        }
                        else
                        {
                            // Other keys are compared pair by pair, those of many candidates through an index of the
                            // constraint's own access method and operator classes.
                            string from = "b";
                            string with = $"WITH b AS MATERIALIZED ({keys}) ";
                            if (many)
                            {
                                from = "worf_held_keys";
                                with = "";
                                bound = false;
                                _ = Worf.Execute($"CREATE TEMPORARY TABLE worf_held_keys AS {keys}", parameters);
                                _ = Worf.Execute($"CREATE INDEX ON worf_held_keys USING {Worf.QuoteIdentifier(constraint.AccessMethod)} " +
                                    $"({string.Join(", ", constraint.OperatorClasses.Select((opclass, k) => $"k{k} {opclass}"))})");
                                _ = Worf.Execute("ANALYZE worf_held_keys");
                            }

                            string renamed = string.Join(", ", Enumerable.Range(0, constraint.Keys.Length).Select(k => $"x.k{k} AS {keyStem}{k}"));
                            sql = $"""
                                {with}{into}(SELECT DISTINCT ON (b.{candidateName}) b.{candidateName}, {number}, {KeyText(constraint.Keys.Length, $"b.{keyStem}")},
                                    {KeyText(constraint.Keys.Length, null, constraint.Compared)}, NULL::pg_catalog.int8, NULL::pg_catalog.int8, NULL::pg_catalog.text
                                FROM (SELECT x.r AS {candidateName}, {renamed} FROM {from} x) b
                                    JOIN {table} t ON {(constraint.Predicate is string joinPredicate ? $"({joinPredicate}) AND " : "")}{constraint.Conflict(null, "b", keyStem)} AND {kept}
                                ORDER BY b.{candidateName})
                                UNION ALL
                                SELECT b.r, {number}, {keyText}, NULL, NULL, a.r, {KeyText(constraint.Keys.Length, "a.k")}
                                FROM {from} a JOIN {from} b ON a.r OPERATOR(pg_catalog.<) b.r AND {constraint.Conflict("a", "b")}
                                """;
                        }

                        if (many)
                        {
                            _ = Worf.Execute(sql, bound ? parameters : []);
                        }
                        else
                        {
                            found.AddRange(Worf.Read(sql, 0, parameters).Select(Conflict));
                        }

                        if (!bound)
                        {
                            _ = Worf.Execute("DROP TABLE worf_held_keys");
                            Worf.EndForDecoding();
                        }
                    }, PgSubtransactionMode.Atomic);
                }
                catch (PgException error) when (!Apply.IsTransient(error))
                {
                    found.Clear();
                }

                gathered.AddRange(found);
            }

            // In the copy's order, a row is refused by the first constraint in which a row the table keeps holds its
            // keys, or an earlier row that's written takes them; a row written takes its keys in every constraint.
            gathered.Sort((a, b) => a.R != b.R ? a.R.CompareTo(b.R) : a.C.CompareTo(b.C));
            if (many)
            {
                _ = Worf.Execute("CREATE INDEX ON pg_temp.worf_held_conflicts (r, c); ANALYZE pg_temp.worf_held_conflicts");
            }

            long conflictsFound = Stopwatch.GetTimestamp();
            Finding = Stopwatch.GetElapsedTime(started, conflictsFound);
            const string ConflictScansSql = """
                SELECT pg_catalog.pg_stat_get_xact_numscans(c.oid), pg_catalog.pg_stat_get_xact_tuples_returned(c.oid) OPERATOR(pg_catalog.+) pg_catalog.pg_stat_get_xact_tuples_fetched(c.oid)
                FROM (SELECT 'pg_temp.worf_held_conflicts'::pg_catalog.regclass::pg_catalog.oid) c (oid)
                """;
            (long scansAt, long rowsAt) = many ? ConflictReads() : (0, 0);

            RefusedChecked = many
                ? Worf.ReadScalar<int>("SELECT pg_catalog.count(DISTINCT h.r)::pg_catalog.int4 FROM pg_temp.worf_held_conflicts h")
                : gathered.Select(conflict => conflict.R).Distinct().Count();
            bool pairs = constraints.Exists(constraint => constraint.Orderings is null);

            // What the rows decided so far took and were refused, for the part being decided: all of it, for few
            // candidates, and otherwise what the temporary tables hold of what the part's candidates conflict with, with
            // what the part adds, which goes to them once it's decided.
            HashSet<long> refusedRows = [];
            Dictionary<(int Constraint, long Same), string> taken = [];
            List<(int Constraint, long Same, string Key)> newlyTaken = [];
            List<long> newlyRefused = [];
            long refusedCount = 0;
            for (long after = long.MinValue; ;)
            {
                List<(long R, int C, string Key, string? Kept, long Same, long? Earlier, string? EarlierKey)> part = many
                    ? [.. Worf.Read($"""
                        SELECT {Columns} FROM pg_temp.worf_held_conflicts h
                        WHERE h.r OPERATOR(pg_catalog.>) $1 AND h.r OPERATOR(pg_catalog.<=) (SELECT max(d.r) FROM (
                            SELECT DISTINCT n.r FROM pg_temp.worf_held_conflicts n WHERE n.r OPERATOR(pg_catalog.>) $1 ORDER BY n.r LIMIT 1000) d)
                        ORDER BY h.r, h.c
                        """, 0, SpiParameter.Create(after)).Select(Conflict)]
                    : after == long.MinValue ? gathered : [];
                if (part.Count == 0)
                {
                    break;
                }

                if (many)
                {
                    taken.Clear();
                    refusedRows.Clear();
                    newlyTaken.Clear();
                    newlyRefused.Clear();
                    List<(int C, long Same)> sames = [.. part.Where(conflict => constraints[conflict.C].Orderings is not null)
                        .Select(conflict => (conflict.C, conflict.Same)).Distinct()];
                    if (sames.Count > 0)
                    {
                        foreach (WorfRow found in Worf.Read("""
                            SELECT w.c, w.same, w.key FROM pg_temp.worf_held_taken w
                                JOIN ROWS FROM (pg_catalog.unnest($1::pg_catalog.int4[]), pg_catalog.unnest($2::pg_catalog.int8[])) p (c, same)
                                    ON p.c OPERATOR(pg_catalog.=) w.c AND p.same OPERATOR(pg_catalog.=) w.same
                            """, 0, SpiParameter.Create(sames.Select(same => same.C).ToArray()), SpiParameter.Create(sames.Select(same => same.Same).ToArray())))
                        {
                            taken[(found.Get<int>(0), found.Get<long>(1))] = found.Get<string>(2);
                        }
                    }

                    long[] earlier = [.. part.Where(conflict => conflict.Earlier is not null).Select(conflict => conflict.Earlier!.Value).Distinct()];
                    if (pairs && earlier.Length > 0)
                    {
                        refusedRows.UnionWith(Worf.Read("SELECT w.r FROM pg_temp.worf_held_refused w WHERE w.r OPERATOR(pg_catalog.=) ANY ($1::pg_catalog.int8[])", 0,
                            SpiParameter.Create(earlier)).Select(found => found.Get<long>(0)));
                    }
                }

                List<(long Row, PgException Error)> refused = [];
                for (int first = 0; first < part.Count;)
                {
                    long candidate = part[first].R;
                    int end = first;
                    while (end < part.Count && part[end].R == candidate)
                    {
                        end++;
                    }

                    if (Refuses(part, first, end) is PgException error)
                    {
                        refused.Add((candidate, error));
                        if (pairs && refusedRows.Add(candidate))
                        {
                            newlyRefused.Add(candidate);
                        }
                    }
                    else
                    {
                        for (int i = first; i < end; i++)
                        {
                            if (constraints[part[i].C].Orderings is not null && taken.TryAdd((part[i].C, part[i].Same), part[i].Key))
                            {
                                newlyTaken.Add((part[i].C, part[i].Same, part[i].Key));
                            }
                        }
                    }

                    first = end;
                }

                after = part[^1].R;
                HeldMost = Math.Max(HeldMost, taken.Count + refusedRows.Count);
                if (many && newlyTaken.Count > 0)
                {
                    _ = Worf.Execute("""
                        INSERT INTO pg_temp.worf_held_taken
                        SELECT * FROM ROWS FROM (pg_catalog.unnest($1::pg_catalog.int4[]), pg_catalog.unnest($2::pg_catalog.int8[]), pg_catalog.unnest($3::pg_catalog.text[]))
                        """, SpiParameter.Create(newlyTaken.Select(entry => entry.Constraint).ToArray()),
                        SpiParameter.Create(newlyTaken.Select(entry => entry.Same).ToArray()), SpiParameter.Create(newlyTaken.Select(entry => entry.Key).ToArray()));
                }

                if (many && newlyRefused.Count > 0)
                {
                    _ = Worf.Execute("INSERT INTO pg_temp.worf_held_refused SELECT pg_catalog.unnest($1::pg_catalog.int8[])",
                        SpiParameter.Create(newlyRefused.ToArray()));
                }

                if (refused.Count > 0)
                {
                    refuse(refused);
                    refusedCount += refused.Count;
                }
            }

            decided = true;
            Deciding = Stopwatch.GetElapsedTime(conflictsFound);
            (long scans, long rows) = many ? ConflictReads() : (0, 0);
            ConflictScans = scans - scansAt;
            ConflictRows = rows - rowsAt;
            return refusedCount;

            // How many times the conflicts were scanned whole, and how many of their rows any scan read, so far.
            static (long Scans, long Rows) ConflictReads()
            {
                WorfRows reads = Worf.Read(ConflictScansSql, 1);
                return (reads[0].Get<long>(0), reads[0].Get<long>(1));
            }

            // The error of the first constraint, in the order they're checked, that refuses a candidate, from its
            // conflicts, part[first] to part[end - 1], in the constraints' order.
            PgException? Refuses(List<(long R, int C, string Key, string? Kept, long Same, long? Earlier, string? EarlierKey)> part, int first, int end)
            {
                for (int i = first; i < end;)
                {
                    int c = part[i].C;
                    int next = i;
                    while (next < end && part[next].C == c)
                    {
                        next++;
                    }

                    string? other = null;
                    for (int k = i; k < next && other is null; k++)
                    {
                        other = part[k].Kept;
                    }

                    for (int k = i; k < next && other is null; k++)
                    {
                        other = constraints[c].Orderings is not null
                            ? taken.GetValueOrDefault((c, part[k].Same))
                            : part[k].Earlier is long earlier && !refusedRows.Contains(earlier) ? part[k].EarlierKey : null;
                    }

                    if (other is not null)
                    {
                        return constraints[c].Error(part[i].Key, other);
                    }

                    i = next;
                }

                return null;
            }
        }
        finally
        {
            // The table is dropped here rather than as the transaction commits, after which nothing may start a
            // subtransaction, as a drop's hooks may; a failure leaves it to the transaction's rollback.
            if (many && decided)
            {
                _ = Worf.Execute("DROP TABLE pg_temp.worf_held_conflicts, pg_temp.worf_held_taken, pg_temp.worf_held_refused");
            }
        }

        static (long, int, string, string?, long, long?, string?) Conflict(WorfRow found)
            => (found.Get<long>(0), found.Get<int>(1), found.Get<string>(2), found.Get<string?>(3), found.Get<long?>(4) ?? 0, found.Get<long?>(5), found.Get<string?>(6));
    }

    /// <summary>
    /// Gets how many candidates the last <see cref="Refused"/> kept to decide on, those that could conflict.
    /// </summary>
    internal static int RefusedChecked { get; private set; }

    /// <summary>
    /// Gets the most keys taken and rows refused that the last <see cref="Refused"/> held in this backend's memory at
    /// once, for a part of its candidates.
    /// </summary>
    internal static int HeldMost { get; private set; }

    /// <summary>
    /// Gets how long the last <see cref="Refused"/> took to find the candidates' conflicts.
    /// </summary>
    internal static TimeSpan Finding { get; private set; }

    /// <summary>
    /// Gets how long the last <see cref="Refused"/> took to decide on the candidates, in the copy's order.
    /// </summary>
    internal static TimeSpan Deciding { get; private set; }

    /// <summary>
    /// Gets how many sequential scans of the candidates' conflicts the last <see cref="Refused"/> made as it decided on
    /// them, a part at a time, which their index spares it.
    /// </summary>
    internal static long ConflictScans { get; private set; }

    /// <summary>
    /// Gets how many rows the last decision read from the conflicts, through any scan, as a bitmap scan reads every
    /// remaining conflict for each part when the conflicts' statistics are missing.
    /// </summary>
    internal static long ConflictRows { get; private set; }

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
