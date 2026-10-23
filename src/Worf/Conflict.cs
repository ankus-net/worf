using Ankus.Postgres;
namespace Worf;

/// <summary>
/// How a conflict was resolved.
/// </summary>
internal enum WorfConflictResolution
{
    /// <summary>
    /// The remote change was applied.
    /// </summary>
    ApplyRemote,

    /// <summary>
    /// The local row was kept.
    /// </summary>
    KeepLocal,

    /// <summary>
    /// The change was skipped.
    /// </summary>
    Skip,
}

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

/// <summary>
/// The kinds of conflict the apply worker detects.
/// </summary>
internal enum WorfConflictType
{
    /// <summary>
    /// An insert found an existing row.
    /// </summary>
    InsertInsert,

    /// <summary>
    /// An update found a row another origin changed.
    /// </summary>
    UpdateUpdate,

    /// <summary>
    /// An update found no row.
    /// </summary>
    UpdateDelete,

    /// <summary>
    /// A delete found no row.
    /// </summary>
    DeleteDelete,
}

/// <summary>
/// worf conflict detection and resolution.
/// </summary>
internal static partial class Conflict
{
    /// <summary>
    /// Gets the method used for conflict resolution for resolvable conflicts.
    /// </summary>
    [PgGucEnum("worf.conflict_resolution", WorfResolveOption.ApplyRemote,
        "Sets method used for conflict resolution for resolvable conflicts.", Context = PgGucContext.SuperuserSet,
        Check = nameof(ConflictResolverCheckHook))]
    internal static partial WorfResolveOption ConflictResolver { get; }

    /// <summary>
    /// Gets the log level used for logging resolved conflicts.
    /// </summary>
    [PgGucEnum("worf.conflict_log_level", ServerMessageLevel.Log, "Sets log level used for logging resolved conflicts.",
        Context = PgGucContext.SuperuserSet)]
    internal static partial ServerMessageLevel ConflictLogLevel { get; }

    /// <summary>
    /// Checks validity of the <c>worf.conflict_resolution</c> setting. Only allow apply_remote when
    /// track_commit_timestamp is off, because there is no way to know where the local tuple originated from.
    /// </summary>
    /// <param name="newval">The proposed value.</param>
    /// <param name="source">The value's source.</param>
    /// <returns>The accepted value, or the rejection.</returns>
    internal static PgGucCheckResult<WorfResolveOption> ConflictResolverCheckHook(WorfResolveOption newval, PgGucSource source)
    {
        bool trackCommitTimestamp;
        unsafe
        {
            trackCommitTimestamp = NativeGlobals.track_commit_timestamp;
        }

        if (!trackCommitTimestamp && newval != WorfResolveOption.ApplyRemote && newval != WorfResolveOption.Error)
        {
            return new(new PgGucCheckError(detail: "track_commit_timestamp is off"));
        }

        return new(newval);
    }
}
