using System.Runtime.CompilerServices;

namespace Worf;

/// <summary>
/// What shared memory holds for the draws under way that hold no switch gate, as 8-byte words, which they read and
/// write without a call into PostgreSQL: the number of switches pending, the number of changes to the blocks told, and
/// then the counts of such draws under way. A backend counts itself in one of those for the moment its draw takes, and
/// a switch waits until all are zero. Each count has a cache line to itself, so backends that draw at once don't slow
/// each other.
/// </summary>
[InlineArray((Slots + 1) * SlotLongs)]
internal struct WorfFastDraws
{
    /// <summary>
    /// The word that holds the number of switches that wait for, or hold, their sequences' gates.
    /// </summary>
    internal const int Pending = 0;

    /// <summary>
    /// The word that holds the number of changes to the blocks told so far.
    /// </summary>
    internal const int Changes = 1;

    /// <summary>
    /// The number of counts, which backends share by their process IDs. The first follows the first cache line.
    /// </summary>
    internal const int Slots = 256;

    /// <summary>
    /// The size of a cache line, in words.
    /// </summary>
    internal const int SlotLongs = 8;

    private long _word;
}
