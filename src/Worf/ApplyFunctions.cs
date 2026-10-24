namespace Worf;

/// <summary>
/// The apply functions, through the relation writer or SPI.
/// </summary>
internal sealed class ApplyFunctions
{
    /// <summary>
    /// Gets the functions that apply changes through the relation writer.
    /// </summary>
    public static ApplyFunctions Heap { get; } = new()
    {
        OnBegin = ApplyHeap.ApplyHeapBegin,
        OnCommit = ApplyHeap.ApplyHeapCommit,
        DoInsert = ApplyHeap.ApplyHeapInsert,
        DoUpdate = ApplyHeap.ApplyHeapUpdate,
        DoDelete = ApplyHeap.ApplyHeapDelete,
        CanMultiInsert = ApplyHeap.ApplyHeapCanMi,
        MultiInsertAddTuple = ApplyHeap.ApplyHeapMiAddTuple,
        MultiInsertFinish = ApplyHeap.ApplyHeapMiFinish,
    };

    /// <summary>
    /// Gets the functions that apply changes through SPI.
    /// </summary>
    public static ApplyFunctions Spi { get; } = new()
    {
        OnBegin = ApplySpi.ApplySpiBegin,
        OnCommit = ApplySpi.ApplySpiCommit,
        DoInsert = ApplySpi.ApplySpiInsert,
        DoUpdate = ApplySpi.ApplySpiUpdate,
        DoDelete = ApplySpi.ApplySpiDelete,
        CanMultiInsert = ApplySpi.ApplySpiCanMi,
        MultiInsertAddTuple = ApplySpi.ApplySpiMiAddTuple,
        MultiInsertFinish = ApplySpi.ApplySpiMiFinish,
    };

    /// <summary>
    /// Gets the function that handles the start of a remote transaction.
    /// </summary>
    public required Action OnBegin { get; init; }

    /// <summary>
    /// Gets the function that handles the end of a remote transaction.
    /// </summary>
    public required Action OnCommit { get; init; }

    /// <summary>
    /// Gets the function that applies an insert.
    /// </summary>
    public required Action<WorfRelation, WorfTupleData> DoInsert { get; init; }

    /// <summary>
    /// Gets the function that applies an update.
    /// </summary>
    public required Action<WorfRelation, WorfTupleData, WorfTupleData> DoUpdate { get; init; }

    /// <summary>
    /// Gets the function that applies a delete.
    /// </summary>
    public required Action<WorfRelation, WorfTupleData> DoDelete { get; init; }

    /// <summary>
    /// Gets the function that decides whether a relation's inserts may be batched.
    /// </summary>
    public required Func<WorfRelation, bool> CanMultiInsert { get; init; }

    /// <summary>
    /// Gets the function that adds an insert to the batch.
    /// </summary>
    public required Action<WorfRelation, WorfTupleData> MultiInsertAddTuple { get; init; }

    /// <summary>
    /// Gets the function that writes the batch.
    /// </summary>
    public required Action<WorfRelation> MultiInsertFinish { get; init; }
}
