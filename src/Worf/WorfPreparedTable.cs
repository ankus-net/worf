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
    /// How many times the managers were asked to note their databases' prepared transactions again, as after a
    /// promotion, which makes the transactions a standby replayed this server's.
    /// </summary>
    public int Rescan;
}
