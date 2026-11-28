namespace Worf;

/// <summary>
/// A statement, or the part of one, whose expressions start, as the sequence range guard notes it: the calls of nextval
/// they make, read once one of them starts, how many of them have started, where PostgreSQL's error context stood as it
/// started, and whether it's only explained.
/// </summary>
/// <param name="draws">Reads the calls: the sequences they name, how many there are, and whether one names none.</param>
/// <param name="context">The error context, as <c>error_context_stack</c> holds it.</param>
/// <param name="explained">Whether the statement is only explained, as EXPLAIN without ANALYZE starts it.</param>
/// <param name="text">Reads the statement's own text, as the client sent it, or null when it has none or has
/// parameters, whose values a plan may hold.</param>
/// <param name="running">The statement running, as a QueryDesc, whose expressions start as it runs, or zero.</param>
/// <param name="planned">Whether a statement's plan starts, whose rows the guard then counts.</param>
/// <param name="compiles">Whether PostgreSQL compiles the plan's expressions with JIT, where it has a JIT provider.</param>
/// <param name="copy">Whether the statement is a COPY FROM, whose tables' expressions start as its rows come.</param>
internal sealed class WorfStartingStatement(Func<(HashSet<uint> Called, int Calls, bool Unnamed)> draws, nint context, bool explained,
    Func<string?>? text = null, nint running = 0, bool planned = false, bool compiles = false, bool copy = false)
{
    private (HashSet<uint> Called, int Calls, bool Unnamed)? _draws;
    private string? _text;

    /// <summary>
    /// Gets the calls of nextval the expressions make.
    /// </summary>
    internal (HashSet<uint> Called, int Calls, bool Unnamed) Draws => _draws ??= draws();

    /// <summary>
    /// Gets the statement's own text, or null.
    /// </summary>
    internal string? Text => _text ??= text?.Invoke();

    /// <summary>
    /// Gets or sets how many of the calls have started.
    /// </summary>
    internal int Looked { get; set; }

    /// <summary>
    /// Gets or sets how many of the calls PostgreSQL has compiled to go through the fmgr hook.
    /// </summary>
    internal int Hooked { get; set; }

    /// <summary>
    /// Gets a value indicating whether the statement is a COPY FROM.
    /// </summary>
    internal bool Copy { get; } = copy;

    /// <summary>
    /// Gets or sets the executor state a COPY FROM writes its rows with, once one of its tables' calls has started.
    /// </summary>
    internal nint CopyState { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a COPY FROM's next row is to look for the calls that started with the
    /// one before.
    /// </summary>
    internal bool CopyWatched { get; set; }

    /// <summary>
    /// Gets or sets PostgreSQL's own state of a COPY FROM, once the calls of its condition and of the domains its
    /// columns hold go through the guard, which its next rows then look for too.
    /// </summary>
    internal nint CopyRead { get; set; }

    /// <summary>
    /// Gets the calls of a COPY FROM that the guard has found so far.
    /// </summary>
    internal HashSet<nint> CopyCalls { get; } = [];

    /// <summary>
    /// Gets or sets what a call that started in a VALUES list's row came to, once one waited for nothing and switched
    /// nothing: the sequences its start counts down, whether its row names none, and what the guard had read then,
    /// its blocks' changes, its own changes, the sequences drawn from without their gates and those the statement
    /// switched. While none of that has changed, the row's next calls come to the same.
    /// </summary>
    internal (uint[] Counted, bool Unnamed, long Changes, long Local, int Ungated, int Switched)? Settled { get; set; }

    /// <summary>
    /// Gets a value indicating whether PostgreSQL compiles the plan's expressions with JIT, where it has a JIT provider.
    /// </summary>
    internal bool Compiles { get; } = compiles;

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

    /// <summary>
    /// Gets the statement running, as a QueryDesc, whose expressions start as it runs, as a VALUES row's, a CHECK's or a
    /// trigger condition's do, or zero for one whose plan starts.
    /// </summary>
    internal nint Running { get; } = running;

    /// <summary>
    /// Gets a value indicating whether a statement's plan starts, before the statement runs, whose rows the guard counts
    /// from the first on.
    /// </summary>
    internal bool Planned { get; } = planned;
}
