namespace Worf;

/// <summary>
/// A table whose local writes and DDL this node holds back while another node changes its definition.
/// </summary>
internal struct WorfFence
{
    /// <summary>
    /// The change's fence, or zero for an unused entry.
    /// </summary>
    public ulong Id;

    /// <summary>
    /// The table's database.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// The table.
    /// </summary>
    public uint Relid;

    /// <summary>
    /// The session through which the changing node asked for the fence, while its transaction hasn't committed, or
    /// zero once it has, after which the fence holds until the change applies here.
    /// </summary>
    public int HolderPid;

    /// <summary>
    /// Whether the fence holds back writes as well as DDL, as it does for a table this node replicates.
    /// </summary>
    public bool BlocksWrites;
}
