using Ankus.Postgres;

namespace Worf;

/// <summary>
/// State related to bulk insert.
/// </summary>
internal sealed unsafe class ApplyMIState
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ApplyMIState"/> class.
    /// </summary>
    /// <param name="rel">The relation being inserted into.</param>
    /// <param name="aestate">The executor state.</param>
    internal ApplyMIState(WorfRelation rel, ApplyExecState aestate)
    {
        Rel = rel;
        Aestate = aestate;
    }

    /// <summary>
    /// Gets the relation being inserted into.
    /// </summary>
    public WorfRelation Rel { get; }

    /// <summary>
    /// Gets the executor state.
    /// </summary>
    public ApplyExecState Aestate { get; }

    /// <summary>
    /// Gets or sets the command the inserts belong to.
    /// </summary>
    public uint Cid { get; set; }

    /// <summary>
    /// Gets or sets the bulk insert state.
    /// </summary>
    public BulkInsertStateData* Bistate { get; set; }

    /// <summary>
    /// Gets or sets the buffered rows' slots.
    /// </summary>
    public TupleTableSlot** BufferedTuples { get; set; }

    /// <summary>
    /// Gets or sets how many rows are buffered before they're written.
    /// </summary>
    public int MaxbufferedTuples { get; set; }

    /// <summary>
    /// Gets or sets how many rows are buffered.
    /// </summary>
    public int NbufferedTuples { get; set; }
}
