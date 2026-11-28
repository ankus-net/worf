namespace Worf;

/// <summary>
/// The header of the shared table of when this server last heard from other nodes.
/// </summary>
internal struct WorfHeardTable
{
    /// <summary>
    /// When the table was set up, at server start: what this server heard before isn't in it.
    /// </summary>
    public long Since;

    /// <summary>
    /// The WAL position this server started from, once read, or zero.
    /// </summary>
    public ulong StartLsn;

    /// <summary>
    /// Whether the data directory's file of starts keeps this server's start, which draws wait for.
    /// </summary>
    public bool StartKept;
}
