namespace Worf;

/// <summary>
/// What apply does with a remote transaction that fails, as <c>worf.exception_behaviour</c> selects.
/// </summary>
internal enum WorfExceptionBehaviour
{
    /// <summary>
    /// Stop with the error, and apply the transaction again when the worker restarts.
    /// </summary>
    [PgGucLabel("error")]
    Error,

    /// <summary>
    /// Apply the transaction again change by change, skipping the changes that fail.
    /// </summary>
    [PgGucLabel("discard")]
    Discard,

    /// <summary>
    /// Skip the whole transaction.
    /// </summary>
    [PgGucLabel("transdiscard")]
    Transdiscard,

    /// <summary>
    /// Disable the subscription.
    /// </summary>
    [PgGucLabel("sub_disable")]
    SubDisable,
}
