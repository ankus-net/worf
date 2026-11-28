using System.Runtime.InteropServices;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// What the sequence range guard keeps for a draw it has bracketed in an expression's steps, where the step before
/// the draw, which PostgreSQL calls straight, reads and writes it without a call into PostgreSQL: the draws the step
/// has taken ahead from those its backend may take before it checks the sequence again, and the draw's own step in
/// both of its forms, PostgreSQL's and the call that goes through the guard.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct WorfDrawBracket
{
    /// <summary>
    /// The draws left of those taken ahead.
    /// </summary>
    public long Budget;

    /// <summary>
    /// The draws taken ahead last.
    /// </summary>
    public long Given;

    /// <summary>
    /// How often the draws left had been set anew as the draws were taken ahead.
    /// </summary>
    public long Epoch;

    /// <summary>
    /// The sequence the draw draws from.
    /// </summary>
    public uint Seqoid;

    /// <summary>
    /// Whether the draw's step is in PostgreSQL's own form.
    /// </summary>
    public bool Direct;

    /// <summary>
    /// Where the step after the draw writes its null flag, which nothing reads.
    /// </summary>
    public bool Unread;

    /// <summary>
    /// The draw's step as PostgreSQL made it.
    /// </summary>
    public ExprEvalStep Own;

    /// <summary>
    /// The draw's step as a call that goes through the guard.
    /// </summary>
    public ExprEvalStep Guarded;
}
