namespace Worf;

/// <summary>
/// A statement, or the part of one, whose expressions start, as the sequence range guard notes it: the calls of nextval
/// they make, read once one of them starts, how many of them have started, where PostgreSQL's error context stood as it
/// started, and whether it's only explained.
/// </summary>
/// <param name="draws">Reads the calls: the sequences they name, how many there are, and whether one names none.</param>
/// <param name="context">The error context, as <c>error_context_stack</c> holds it.</param>
/// <param name="explained">Whether the statement is only explained, as EXPLAIN without ANALYZE starts it.</param>
internal sealed class WorfStartingStatement(Func<(HashSet<uint> Called, int Calls, bool Unnamed)> draws, nint context, bool explained)
{
    private (HashSet<uint> Called, int Calls, bool Unnamed)? _draws;

    /// <summary>
    /// Gets the calls of nextval the expressions make.
    /// </summary>
    internal (HashSet<uint> Called, int Calls, bool Unnamed) Draws => _draws ??= draws();

    /// <summary>
    /// Gets or sets how many of the calls have started.
    /// </summary>
    internal int Looked { get; set; }

    /// <summary>
    /// Gets the managed sequences the statement has waited for a switch of, as it holds no switch gate, once each.
    /// </summary>
    internal HashSet<uint> Switched { get; } = [];

    /// <summary>
    /// Gets the error context as the statement started.
    /// </summary>
    internal nint Context { get; } = context;

    /// <summary>
    /// Gets a value indicating whether the statement is only explained.
    /// </summary>
    internal bool Explained { get; } = explained;
}
