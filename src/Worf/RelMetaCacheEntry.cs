namespace Worf;

/// <summary>
/// Whether the client has a relation's metadata cached.
/// </summary>
internal sealed class RelMetaCacheEntry
{
    /// <summary>
    /// Gets the relation.
    /// </summary>
    public uint Relid { get; init; }

    /// <summary>
    /// Gets or sets whether the client has this relation cached.
    /// </summary>
    public bool IsCached { get; set; }

    /// <summary>
    /// Gets or sets whether the entry is valid and not due to be purged.
    /// </summary>
    public bool IsValid { get; set; }
}
