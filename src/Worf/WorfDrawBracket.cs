using System.Runtime.InteropServices;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// What the sequence range guard keeps for a draw it has bracketed in an expression's steps, where the step before
/// the draw, which PostgreSQL calls straight, reads and writes it without a call into PostgreSQL: the draws the step
/// has taken ahead from those its backend may take before it checks the sequence again, and the draw's own step in
/// each of its forms: PostgreSQL's, the call that goes through the guard, and, for a default's call of nextval, the
/// step PostgreSQL draws an identity column's value with, which doesn't check that the user may draw.
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
    /// The user PostgreSQL's own call last checked may draw from the sequence, or zero.
    /// </summary>
    public uint CheckedBy;

    /// <summary>
    /// The user the draw before was left for PostgreSQL's own call to check, or zero.
    /// </summary>
    public uint CheckingBy;

    /// <summary>
    /// How often this backend had heard of a change to what a privilege rests on when that check passed.
    /// </summary>
    public long CheckedAt;

    /// <summary>
    /// How often this backend had heard of such a change when the draw before was left for PostgreSQL's own call.
    /// </summary>
    public long CheckingAt;

    /// <summary>
    /// The form the draw's step is in: the call that goes through the guard, PostgreSQL's own, or the one that
    /// checks no privilege.
    /// </summary>
    public byte Form;

    /// <summary>
    /// Whether the draw has a form that checks no privilege.
    /// </summary>
    public bool Unchecks;

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

    /// <summary>
    /// The draw's step as PostgreSQL draws an identity column's value, which checks no privilege.
    /// </summary>
    public ExprEvalStep Unchecked;
}
