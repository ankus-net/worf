namespace Worf;

/// <summary>
/// An immediate unique index or exclusion constraint of a table, as the checks of a copy's rows against the rows the
/// table holds and against each other compare them.
/// </summary>
/// <param name="Name">The index's name, which PostgreSQL's errors name.</param>
/// <param name="Exclusion">Whether it's an exclusion constraint rather than a unique index.</param>
/// <param name="Keys">Its key columns and expressions, as PostgreSQL shows them, which read the table's columns
/// unqualified.</param>
/// <param name="Collations">The <c>COLLATE</c> clause each key is compared under, or an empty string.</param>
/// <param name="Operators">The operator each key is compared with, written with its schema: the equality operator of a
/// unique index's operator class, or the exclusion constraint's.</param>
/// <param name="Orderings">The btree ordering operator whose equality each key's operator is, written with its schema,
/// when every key's is one, so that rows that conflict are those with the same keys; otherwise null.</param>
/// <param name="Predicate">The index's predicate, or null.</param>
/// <param name="NullsEqual">Whether a unique index takes nulls as equal.</param>
/// <param name="AccessMethod">The index's access method.</param>
/// <param name="OperatorClasses">Each key's operator class, written with its schema.</param>
/// <param name="Reads">The table's columns its keys and predicate read.</param>
internal sealed record HeldConstraint(string Name, bool Exclusion, string[] Keys, string[] Collations, string[] Operators, string[]? Orderings,
    string? Predicate, bool NullsEqual, string AccessMethod, string[] OperatorClasses, string[] Reads)
{
    /// <summary>
    /// Gets each key as it's compared, read from the columns of the relation in scope, unqualified.
    /// </summary>
    internal IEnumerable<string> Compared => Keys.Select((key, k) => $"(({key}){Collations[k]})");

    /// <summary>
    /// Gets the condition that the keys of a row of the relation in scope, unqualified, conflict with another's.
    /// </summary>
    /// <param name="left">The alias of the keys of the one row, whose columns are k0, k1 and so on, or null for the
    /// relation in scope's.</param>
    /// <param name="right">The alias of the other row's keys.</param>
    /// <returns>The condition.</returns>
    internal string Conflict(string? left, string right) => string.Join(" AND ", Compared.Select((key, k) =>
    {
        string mine = left is null ? key : $"{left}.k{k}";
        string compared = $"{mine} {Operators[k]} {right}.k{k}";
        return NullsEqual ? $"({compared} OR ({mine} IS NOT DISTINCT FROM NULL AND {right}.k{k} IS NOT DISTINCT FROM NULL))" : compared;
    }));

    /// <summary>
    /// Gets the error PostgreSQL raises for a row whose keys conflict with another's.
    /// </summary>
    /// <param name="key">The text of the row's keys.</param>
    /// <param name="other">The text of the other row's keys.</param>
    /// <returns>The error.</returns>
    internal PgException Error(string key, string other)
    {
        string columns = string.Join(", ", Keys);
        return Exclusion
            ? new PgException(PgSqlStates.ExclusionViolation, $"conflicting key value violates exclusion constraint \"{Name}\"",
                detail: $"Key ({columns})=({key}) conflicts with existing key ({columns})=({other}).")
            : new PgException(PgSqlStates.UniqueViolation, $"duplicate key value violates unique constraint \"{Name}\"",
                detail: $"Key ({columns})=({key}) already exists.");
    }
}
