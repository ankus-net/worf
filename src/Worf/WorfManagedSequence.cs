namespace Worf;

/// <summary>
/// A sequence a provider manages, as a new subscription's sync joins it.
/// </summary>
/// <param name="Name">The sequence's qualified name.</param>
/// <param name="Owner">Its owner.</param>
/// <param name="Epoch">Its epoch.</param>
/// <param name="RangeSize">The size of a node's first block.</param>
/// <param name="MaxRangeSize">The largest block a node requests.</param>
/// <param name="RootStart">Where its root may start.</param>
/// <param name="RootEnd">Where its root ends.</param>
internal sealed record WorfManagedSequence(string Name, uint Owner, int Epoch, long RangeSize, long MaxRangeSize, long? RootStart, long? RootEnd);
