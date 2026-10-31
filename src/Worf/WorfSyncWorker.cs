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
    /// The position the worker waits for the apply worker to reach before it copies the table, or zero.
    /// </summary>
    public ulong CopyLsn;

    /// <summary>
    /// The position before which the apply worker has applied everything that committed, as it last told the worker
    /// while it waits.
    /// </summary>
    public ulong AppliedLsn;
}
