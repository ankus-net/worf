using System.Runtime.CompilerServices;

namespace Worf;

/// <summary>
/// The shared counts of changes to the managed sequences that have no counter of their own, each kept by those whose
/// database and OID hash to it, so that such a change counts for only the few sequences that share its count.
/// </summary>
[InlineArray(256)]
internal struct WorfTouchOverflow
{
    private long _count;
}
