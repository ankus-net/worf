namespace Worf;

/// <summary>
/// A worker slot in shared memory.
/// </summary>
internal struct WorfWorker
{
    /// <summary>
    /// The worker's type.
    /// </summary>
    public WorfWorkerType WorkerType;

    /// <summary>
    /// Generation counter incremented at each registration.
    /// </summary>
    public ushort Generation;

    /// <summary>
    /// Pointer to proc array. Zero if not running.
    /// </summary>
    public nint Proc;

    /// <summary>
    /// Time at which worker crashed (normally 0).
    /// </summary>
    public long CrashedAt;

    /// <summary>
    /// Database id to connect to.
    /// </summary>
    public uint Dboid;

    /// <summary>
    /// Set by manager before exiting due to missing extension. Prevents supervisor from being woken immediately,
    /// avoiding rapid restart cycles for databases without worf.
    /// </summary>
    public bool SkipSupervisorWakeup;

    /// <summary>
    /// Set when another process asks the worker to stop. pglogical sends its workers SIGTERM, whose handler only
    /// sets <c>got_SIGTERM</c> and the process latch; this flag and the latch do the same.
    /// </summary>
    public bool TerminateRequested;

    /// <summary>
    /// Type-specific worker info.
    /// </summary>
    public WorfWorkerInfo Worker;
}
