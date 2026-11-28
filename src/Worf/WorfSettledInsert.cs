namespace Worf;

/// <summary>
/// What the sequence range guard keeps of an INSERT of one row whose guarded columns take their defaults, once it has
/// run in full: where the plan draws, and what the plan and the session looked like then. The statement's next runs,
/// as each turn of a loop makes one, go by it while nothing it rests on has changed, in place of reading the plan,
/// the table and the calls again.
/// </summary>
internal sealed class WorfSettledInsert
{
    /// <summary>
    /// Gets the plan's top node, which a plan made anew for the statement replaces.
    /// </summary>
    internal required nint Tree { get; init; }

    /// <summary>
    /// Gets where the text the statement came in is, and the statement's place and length in it.
    /// </summary>
    internal required (nint Source, int Location, int Length) Text { get; init; }

    /// <summary>
    /// Gets a hash of the statement's own text, which tells another statement that later stands where it stood.
    /// </summary>
    internal required long Hash { get; init; }

    /// <summary>
    /// Gets how often a table, a type, a function or a schema had changed, and the blocks, as the statement ran.
    /// </summary>
    internal required (long Tables, long Blocks, long Local) Stamp { get; init; }

    /// <summary>
    /// Gets the search path the statement ran under.
    /// </summary>
    internal required string SearchPath { get; init; }

    /// <summary>
    /// Gets the draws the plan makes for each row, by the step each stands at in the expression that works the row
    /// out, as PostgreSQL builds it, the last first, with the sequence each draws from and whether it is an identity
    /// column's, which is no call of nextval.
    /// </summary>
    internal required (int Step, uint Seqoid, bool Identity)[] Draws { get; init; }

    /// <summary>
    /// Gets the column whose value the guard's trigger sets against the value drawn, where the table's own triggers
    /// may change a row, or zero where the rows skip the trigger.
    /// </summary>
    internal required short Verifies { get; init; }
}
