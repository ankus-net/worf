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
    /// while it waits, or as it last reported to the provider when the worker started to wait.
    /// </summary>
    public ulong AppliedLsn;

    /// <summary>
    /// The position of the last change the apply worker applied of the transaction that commits at
    /// <see cref="AppliedLsn"/>, as it last told the worker while it waits, or zero.
    /// </summary>
    public ulong ChangeLsn;
}
