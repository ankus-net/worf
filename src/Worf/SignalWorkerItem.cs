namespace Worf;

/// <summary>
/// A worker to signal when the transaction commits.
/// </summary>
/// <param name="SubId">The subscription.</param>
/// <param name="Kill">Whether to stop the worker.</param>
internal readonly record struct SignalWorkerItem(uint SubId, bool Kill);
