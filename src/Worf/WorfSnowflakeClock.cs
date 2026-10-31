namespace Worf;

/// <summary>
/// A database's Snowflake clock: the last time and counter it issued, and how far the time is reserved durably.
/// </summary>
internal struct WorfSnowflakeClock
{
    /// <summary>
    /// The database, or zero for an unused entry.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// Whether the range worker has raised the clock to the time reserved before the server started.
    /// </summary>
    public bool Floored;

    /// <summary>
    /// The last time, in milliseconds since 2025, shifted left by the counter's bits, plus the counter.
    /// </summary>
    public long LastState;

    /// <summary>
    /// The node number the last ID carried.
    /// </summary>
    public int LastNode;

    /// <summary>
    /// The time up to which IDs may be issued, which the range worker has written durably.
    /// </summary>
    public long ReservedMs;

    /// <summary>
    /// Whether a session waits for the range worker to reserve the time it needs.
    /// </summary>
    public bool Wanted;
}
