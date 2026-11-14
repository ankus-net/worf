namespace Worf;

/// <summary>
/// An exchange with a subscription's provider that the exchange worker has under way without waiting on it: the
/// connection it makes, if it keeps none for the provider, and then each call, with what the call's message carried.
/// </summary>
/// <param name="sub">The subscription.</param>
/// <param name="waiting">What the link waits on, as the exchange started, or null.</param>
/// <param name="fenced">Whether the provider's fence held, as the exchange started.</param>
/// <param name="startedAt">When the exchange started, as a timestamp.</param>
internal sealed class WorfPendingExchange(WorfSubscription sub, string? waiting, bool fenced, long startedAt)
{
    /// <summary>
    /// Gets the subscription.
    /// </summary>
    internal WorfSubscription Sub { get; } = sub;

    /// <summary>
    /// Gets what the link waits on, or null.
    /// </summary>
    internal string? Waiting { get; } = waiting;

    /// <summary>
    /// Gets a value indicating whether the provider's fence held.
    /// </summary>
    internal bool Fenced { get; } = fenced;

    /// <summary>
    /// Gets when the exchange started, as a timestamp.
    /// </summary>
    internal long StartedAt { get; } = startedAt;

    /// <summary>
    /// Gets or sets the connection being made, or null.
    /// </summary>
    internal WorfProbe? Probe { get; set; }

    /// <summary>
    /// Gets or sets the connection string the probe connects with, and the exchange's timeout as it started, which the
    /// connection is kept with once it's made.
    /// </summary>
    internal (string Conninfo, int Timeout) ProbedWith { get; set; }

    /// <summary>
    /// Gets or sets the call under way, or null.
    /// </summary>
    internal WorfPendingQuery? Query { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the connection is a probe of a provider whose last call ran out of time.
    /// </summary>
    internal bool Probing { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the call goes over a connection kept from an earlier exchange, which is
    /// made again, once, if it has failed meanwhile.
    /// </summary>
    internal bool Reused { get; set; }

    /// <summary>
    /// Gets or sets how far this node had applied the provider's changes as the call's message was made.
    /// </summary>
    internal ulong Applied { get; set; }

    /// <summary>
    /// Gets or sets the refusals the call's message carried, acknowledged once the reply has come.
    /// </summary>
    internal List<Guid> Refusals { get; set; } = [];

    /// <summary>
    /// Gets or sets how many calls the exchange has made.
    /// </summary>
    internal int Calls { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this node received a block in the exchange.
    /// </summary>
    internal bool Received { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this node granted a block in the exchange.
    /// </summary>
    internal bool Granted { get; set; }
}
