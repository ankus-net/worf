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
    /// Gets or sets, for each remote column whose type differs from its local column's, the local OID of the remote
    /// type, with which its binary and internal values are decoded, or zero. Filled with the local relation.
    /// </summary>
    public uint[] DecodeTypes { get; set; } = [];

    /// <summary>
    /// Gets or sets, for each remote column of a type this node lacks, the type's name, or null. Filled with the local
    /// relation.
    /// </summary>
    public string?[] MissingTypes { get; set; } = [];

    /// <summary>
    /// Gets or sets whether the local relation has row triggers that fire for replicated changes. Only valid as
    /// long as relation mapping is.
    /// </summary>
    public bool HasTriggers { get; set; }
}
