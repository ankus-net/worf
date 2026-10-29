namespace Worf;

/// <summary>
/// The header of the shared table of fences.
/// </summary>
internal unsafe struct WorfFenceTable
{
    /// <summary>
    /// The most databases whose kept fences the table records it has read since the server started.
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
    /// The databases whose kept fences were read back from worf.fence since the server started.
    /// </summary>
    public fixed uint Loaded[MaxDatabases];
}
