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
}
