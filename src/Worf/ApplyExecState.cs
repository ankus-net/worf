using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The executor state for applying a change to a relation.
/// </summary>
internal sealed unsafe class ApplyExecState
{
    /// <summary>
    /// Gets or sets the executor state.
    /// </summary>
    public EState* Estate { get; set; }

    /// <summary>
    /// Gets or sets the recheck state for row triggers.
    /// </summary>
    public EPQState* Epqstate { get; set; }

    /// <summary>
    /// Gets or sets the relation's result info.
    /// </summary>
    public ResultRelInfo* ResultRelInfo { get; set; }

    /// <summary>
    /// Gets or sets the slot holding the row to apply.
    /// </summary>
    public TupleTableSlot* Slot { get; set; }
}
