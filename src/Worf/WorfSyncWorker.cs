using Ankus.Postgres;

namespace Worf;

/// <summary>
/// A sync worker's shared state.
/// </summary>
internal struct WorfSyncWorker
{
    /// <summary>
    /// Apply worker info, must be first.
    /// </summary>
    public WorfApplyWorker Apply;

    /// <summary>
    /// Name of the schema of table to copy if any.
    /// </summary>
    public nameData Nspname;

    /// <summary>
    /// Name of the table to copy if any.
    /// </summary>
    public nameData Relname;

    /// <summary>
    /// The position of the copy's snapshot, once the worker waits for the apply worker to reach it, or zero.
    /// </summary>
    public ulong CopyLsn;

    /// <summary>
    /// Whether the apply worker has applied everything that committed before the copy's snapshot, so the copy can
    /// start.
    /// </summary>
    public bool CopyAllowed;
}
