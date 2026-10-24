namespace Worf;

/// <summary>
/// How a table replicates through the replication sets of one connection.
/// </summary>
internal sealed class WorfTableRepInfo
{
    /// <summary>
    /// Gets or sets the table.
    /// </summary>
    public uint Reloid { get; set; }

    /// <summary>
    /// Gets or sets whether this entry is valid.
    /// </summary>
    public bool IsValid { get; set; }

    /// <summary>
    /// Gets or sets whether inserts should be replicated.
    /// </summary>
    public bool ReplicateInsert { get; set; }

    /// <summary>
    /// Gets or sets whether updates should be replicated.
    /// </summary>
    public bool ReplicateUpdate { get; set; }

    /// <summary>
    /// Gets or sets whether deletes should be replicated.
    /// </summary>
    public bool ReplicateDelete { get; set; }

    /// <summary>
    /// Gets or sets the column filter: null if everything is replicated, otherwise the replicated columns' numbers.
    /// </summary>
    public HashSet<int>? AttList { get; set; }

    /// <summary>
    /// Gets the compiled row filter nodes, allocated in <c>CacheMemoryContext</c>.
    /// </summary>
    public List<nint> RowFilter { get; } = [];
}
