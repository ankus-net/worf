namespace Worf;

/// <summary>
/// A replication set.
/// </summary>
internal sealed class WorfRepSet
{
    /// <summary>
    /// Gets or sets the set's id.
    /// </summary>
    public uint Id { get; set; }

    /// <summary>
    /// Gets or sets the node the set belongs to.
    /// </summary>
    public uint NodeId { get; set; }

    /// <summary>
    /// Gets or sets the set's name.
    /// </summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Gets or sets whether the set replicates inserts.
    /// </summary>
    public bool ReplicateInsert { get; set; }

    /// <summary>
    /// Gets or sets whether the set replicates updates.
    /// </summary>
    public bool ReplicateUpdate { get; set; }

    /// <summary>
    /// Gets or sets whether the set replicates deletes.
    /// </summary>
    public bool ReplicateDelete { get; set; }

    /// <summary>
    /// Gets or sets whether the set replicates truncates.
    /// </summary>
    public bool ReplicateTruncate { get; set; }
}
