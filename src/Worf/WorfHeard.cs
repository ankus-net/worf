namespace Worf;

/// <summary>
/// When this server last heard from another node of a database that manages sequence ranges.
/// </summary>
internal struct WorfHeard
{
    /// <summary>
    /// The database, or zero for an unused entry.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// The node.
    /// </summary>
    public uint NodeId;

    /// <summary>
    /// When the node last exchanged with this server, or zero.
    /// </summary>
    public long DirectAt;

    /// <summary>
    /// The newest time that a report the node made carries, received directly or relayed, or zero.
    /// </summary>
    public long ReportedAt;

    /// <summary>
    /// The system identifier the node's WAL positions below belong to.
    /// </summary>
    public ulong Sysid;

    /// <summary>
    /// The timeline the node's WAL positions below belong to.
    /// </summary>
    public long Timeline;

    /// <summary>
    /// The WAL position the node's last call to this server carried, or zero.
    /// </summary>
    public ulong CallLsn;

    /// <summary>
    /// The WAL position the node's last answer to this server carried, or zero.
    /// </summary>
    public ulong ReplyLsn;
}
