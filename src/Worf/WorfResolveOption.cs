namespace Worf;

/// <summary>
/// The methods <c>worf.conflict_resolution</c> selects for resolvable conflicts.
/// </summary>
internal enum WorfResolveOption
{
    /// <summary>
    /// Raise an error.
    /// </summary>
    [PgGucLabel("error")]
    Error,

    /// <summary>
    /// Apply the remote change.
    /// </summary>
    [PgGucLabel("apply_remote")]
    ApplyRemote,

    /// <summary>
    /// Keep the local row.
    /// </summary>
    [PgGucLabel("keep_local")]
    KeepLocal,

    /// <summary>
    /// Keep the row with the later commit timestamp.
    /// </summary>
    [PgGucLabel("last_update_wins")]
    LastUpdateWins,

    /// <summary>
    /// Keep the row with the earlier commit timestamp.
    /// </summary>
    [PgGucLabel("first_update_wins")]
    FirstUpdateWins,
}
