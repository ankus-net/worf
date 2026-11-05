namespace Worf;

/// <summary>
/// What PostgreSQL's <c>CompareIndexInfo</c> compares as a partitioned index takes a partition's index for its own,
/// that worf can read from a statement and the catalog alike: whether the index is unique and treats nulls as not
/// distinct, its access method, its columns in order, the included ones after its keys, with an empty name for an
/// expression, how many are keys, and whether it has a predicate or is an exclusion constraint's, which PostgreSQL never
/// takes. Expressions, predicates, collations and operator families aren't compared, so that indexes they alone tell
/// apart may match.
/// </summary>
/// <param name="Unique">Whether the index is unique.</param>
/// <param name="NullsNotDistinct">Whether it treats nulls as not distinct.</param>
/// <param name="AccessMethod">Its access method's name.</param>
/// <param name="Columns">Its columns' names, with an empty one for an expression.</param>
/// <param name="KeyColumns">How many of them are keys.</param>
/// <param name="Predicate">Whether it has a predicate.</param>
/// <param name="Exclusion">Whether it's an exclusion constraint's.</param>
internal sealed record IndexShape(bool Unique, bool NullsNotDistinct, string AccessMethod, string[] Columns, int KeyColumns, bool Predicate, bool Exclusion)
{
    /// <summary>
    /// Checks whether PostgreSQL may take an index of this shape for one of another.
    /// </summary>
    /// <param name="other">The other index's shape.</param>
    /// <returns>Whether it may.</returns>
    internal bool MayMatch(IndexShape other)
        => !Exclusion && !other.Exclusion && Unique == other.Unique && NullsNotDistinct == other.NullsNotDistinct &&
            AccessMethod == other.AccessMethod && KeyColumns == other.KeyColumns && Predicate == other.Predicate &&
            Columns.SequenceEqual(other.Columns);
}
