namespace Worf.CreateSubscriber;

/// <summary>
/// What a ping found, as libpq's <c>PGPing</c> reports it.
/// </summary>
internal enum PingStatus
{
    /// <summary>
    /// The server is accepting connections.
    /// </summary>
    Ok,

    /// <summary>
    /// The server is alive but rejecting connections, such as while it starts.
    /// </summary>
    Reject,

    /// <summary>
    /// The server could not be contacted.
    /// </summary>
    NoResponse,

    /// <summary>
    /// The connection parameters were invalid, so no attempt was made.
    /// </summary>
    NoAttempt,
}
