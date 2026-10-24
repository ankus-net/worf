namespace Worf;

/// <summary>
/// A subscription's or table's synchronization status.
/// </summary>
internal sealed class WorfSyncStatus
{
    /// <summary>
    /// Gets or sets the kind of synchronization.
    /// </summary>
    public char Kind { get; set; }

    /// <summary>
    /// Gets or sets the subscription.
    /// </summary>
    public uint Subid { get; set; }

    /// <summary>
    /// Gets or sets the table's schema, empty for the subscription.
    /// </summary>
    public string Nspname { get; set; } = "";

    /// <summary>
    /// Gets or sets the table's name, empty for the subscription.
    /// </summary>
    public string Relname { get; set; } = "";

    /// <summary>
    /// Gets or sets the synchronization status.
    /// </summary>
    public char Status { get; set; }

    /// <summary>
    /// Gets or sets the position the status was reached at.
    /// </summary>
    public ulong Statuslsn { get; set; }
}
