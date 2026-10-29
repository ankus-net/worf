namespace Worf;

/// <summary>
/// A sequence's definition, as range management reads it.
/// </summary>
/// <param name="seqoid">The sequence.</param>
/// <param name="name">Its quoted, schema-qualified name.</param>
/// <param name="typeOid">Its type: <c>smallint</c>, <c>integer</c> or <c>bigint</c>.</param>
/// <param name="increment">Its increment.</param>
/// <param name="min">Its minimum value.</param>
/// <param name="max">Its maximum value.</param>
/// <param name="cycle">Whether it cycles.</param>
/// <param name="persistence">Its relation's persistence: <c>p</c>, <c>u</c> or <c>t</c>.</param>
internal sealed class SequenceInfo(uint seqoid, string name, uint typeOid, long increment, long min, long max, bool cycle, char persistence)
{
    /// <summary>
    /// Gets the sequence.
    /// </summary>
    public uint Seqoid { get; } = seqoid;

    /// <summary>
    /// Gets its quoted, schema-qualified name.
    /// </summary>
    public string Name { get; } = name;

    /// <summary>
    /// Gets its type.
    /// </summary>
    public uint TypeOid { get; } = typeOid;

    /// <summary>
    /// Gets its increment.
    /// </summary>
    public long Increment { get; } = increment;

    /// <summary>
    /// Gets its minimum value.
    /// </summary>
    public long Min { get; } = min;

    /// <summary>
    /// Gets its maximum value.
    /// </summary>
    public long Max { get; } = max;

    /// <summary>
    /// Gets whether it cycles.
    /// </summary>
    public bool Cycle { get; } = cycle;

    /// <summary>
    /// Gets its relation's persistence.
    /// </summary>
    public char Persistence { get; } = persistence;

    /// <summary>
    /// Gets whether it counts upwards.
    /// </summary>
    public bool Ascending => Increment > 0;

    /// <summary>
    /// Gets the two values quarantine sets its bounds to, which no root contains: its type's two smallest when it
    /// counts upwards, and two largest otherwise. Its position is the one nearer the roots, so nextval fails.
    /// </summary>
    public (long Lo, long Hi) Quarantine => Ascending
        ? (SequenceRanges.TypeRange(TypeOid).Min, SequenceRanges.TypeRange(TypeOid).Min + 1)
        : (SequenceRanges.TypeRange(TypeOid).Max - 1, SequenceRanges.TypeRange(TypeOid).Max);

    /// <summary>
    /// Gets the reserved value nearest the roots, which every root starts beyond.
    /// </summary>
    public long QuarantineEdge => Ascending ? Quarantine.Hi : Quarantine.Lo;
}
