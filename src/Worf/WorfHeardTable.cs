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
}
