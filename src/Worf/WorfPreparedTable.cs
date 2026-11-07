namespace Worf;

/// <summary>
/// The header of the shared table of prepared transactions whose end wakes managers.
/// </summary>
internal struct WorfPreparedTable
{
    /// <summary>
    /// The number of entries in use.
    /// </summary>
    public int Used;

    /// <summary>
    /// When the table started, with this server's shared memory.
    /// </summary>
    public long StartedAt;
}
