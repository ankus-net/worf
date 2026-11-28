namespace Worf;

/// <summary>
/// The header of the shared table of fences.
/// </summary>
internal unsafe struct WorfFenceTable
{
    /// <summary>
    /// The most databases that the table notes as having had their kept fences read back, a cache from which a
    /// database with no fences is forgotten when it's full.
    /// </summary>
    public const int MaxDatabases = 64;

    /// <summary>
    /// The number of entries in use.
    /// </summary>
    public int Used;

    /// <summary>
    /// The number of databases in <see cref="Loaded"/>.
    /// </summary>
    public int LoadedCount;

    /// <summary>
    /// How many times fences were let go of, which a read of worf.fence that a release overtook notices.
    /// </summary>
    public long Generation;

    /// <summary>
    /// The databases whose kept fences were read back from worf.fence since the server started.
    /// </summary>
    public fixed uint Loaded[MaxDatabases];
}
