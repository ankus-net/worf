using Ankus.Postgres;

namespace Worf;

/// <summary>
/// A remote relation and its mapping to the local relation.
/// </summary>
internal sealed unsafe class WorfRelation
{
    /// <summary>
    /// Gets or sets the remote relation's id.
    /// </summary>
    public uint RemoteId { get; set; }

    /// <summary>
    /// Gets or sets the relation's schema.
    /// </summary>
    public string Nspname { get; set; } = "";

    /// <summary>
    /// Gets or sets the relation's name.
    /// </summary>
    public string Relname { get; set; } = "";

    /// <summary>
    /// Gets or sets the remote columns' names.
    /// </summary>
    public string[] Attnames { get; set; } = [];

    /// <summary>
    /// Gets the number of remote columns.
    /// </summary>
    public int Natts => Attnames.Length;

    /// <summary>
    /// Gets or sets the local relation, filled as needed.
    /// </summary>
    public uint Reloid { get; set; }

    /// <summary>
    /// Gets or sets whether the local relation and the mapping to it are up to date. An invalidation of the local
    /// relation clears it, so the relation is looked up by name again the next time it's opened.
    /// </summary>
    public bool IsValid { get; set; }

    /// <summary>
    /// Gets or sets the open local relation.
    /// </summary>
    public RelationData* Rel { get; set; }

    /// <summary>
    /// Gets or sets the local column index of each remote column.
    /// </summary>
    public int[] Attmap { get; set; } = [];

    /// <summary>
    /// Gets or sets each remote column's type, or null when the provider didn't say.
    /// </summary>
    public WorfRemoteType?[] RemoteTypes { get; set; } = [];

    /// <summary>
    /// Gets or sets how each remote column's values convert to its local column's type, or null for a column whose
    /// type reads them. Filled with the local relation.
    /// </summary>
    public WorfColumnConversion?[] Conversions { get; set; } = [];

    /// <summary>
    /// Gets or sets whether the conversions depend on types and functions other than the local columns' own: the
    /// remote relation has a type that isn't PostgreSQL's own, or a column that converts.
    /// </summary>
    public bool ConversionsDependOnTypes { get; set; }

    /// <summary>
    /// Gets or sets the count of changes to types and functions the conversions were found after.
    /// </summary>
    public long ConversionsStamp { get; set; }

    /// <summary>
    /// Gets or sets whether the local relation has row triggers that fire for replicated changes. Only valid as
    /// long as relation mapping is.
    /// </summary>
    public bool HasTriggers { get; set; }

    /// <summary>
    /// Gets or sets the deferrable indexes of the local relation whose rechecks apply needs and doesn't get, under
    /// session_replication_role replica, and can't do itself: those with expressions or a predicate, or null. Only
    /// valid as long as relation mapping is.
    /// </summary>
    public string? DeferrableIndexes { get; set; }

    /// <summary>
    /// Gets or sets the checks apply runs itself after it writes a row, for the deferrable unique and exclusion
    /// constraints of the local relation on its columns, whose rechecks it doesn't get, or null: each constraint's name,
    /// whether it excludes, the query that tells whether a row other than the one written conflicts with it, and the
    /// columns, by number, whose values, as bytea, are the query's parameters. Only valid as long as relation mapping is.
    /// </summary>
    public List<(string Name, bool Exclusion, string Query, int[] Columns)>? DeferrableChecks { get; set; }

    /// <summary>
    /// Gets the type a local column's values are sent in.
    /// </summary>
    /// <param name="attid">The local column.</param>
    /// <returns>The type, or null when the provider didn't say or doesn't send the column.</returns>
    public WorfRemoteType? RemoteType(int attid)
    {
        int i = Array.IndexOf(Attmap, attid);
        return i < 0 ? null : RemoteTypes[i];
    }
}
