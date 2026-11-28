namespace Worf;

/// <summary>
/// The conditions the sequence range guard has a table's rows checked under where the table's guard triggers hold none
/// themselves, built once for the session until the table changes: PostgreSQL would otherwise copy a trigger's
/// condition, as text, for every statement that writes the table, and build it again for each.
/// </summary>
internal sealed class WorfGuardConditions
{
    /// <summary>
    /// Gets or sets the memory context that holds the conditions.
    /// </summary>
    internal nint Context { get; set; }

    /// <summary>
    /// Gets or sets the condition a new row is checked under, as an ExprState, or zero until a statement needs it.
    /// </summary>
    internal nint Insert { get; set; }

    /// <summary>
    /// Gets or sets the condition an updated row is checked under, as an ExprState, or zero until a statement needs it.
    /// </summary>
    internal nint Update { get; set; }

    /// <summary>
    /// Gets or sets the condition a new row is checked under where a trigger may have changed it: one that first sets
    /// the row's value against the value its default drew, as an ExprState, zero until a statement needs it, or -1
    /// where the table has none.
    /// </summary>
    internal nint Noting { get; set; }

    /// <summary>
    /// Gets or sets where <see cref="Noting"/> reads the value a row's default drew.
    /// </summary>
    internal nint Drawn { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the table has changed since the conditions were built.
    /// </summary>
    internal bool Stale { get; set; }
}
