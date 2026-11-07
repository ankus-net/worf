using System.Collections;

namespace Worf;

/// <summary>
/// The rows a query of worf's returned, as <see cref="SpiResult"/> holds them, whose text cells read exactly, through
/// <see cref="WorfRow"/>.
/// </summary>
internal sealed class WorfRows : IReadOnlyList<WorfRow>
{
    private readonly SpiResult? _result;
    private readonly List<PgHeapTuple>? _tuples;

    /// <summary>
    /// Initializes the rows of one Ankus query.
    /// </summary>
    /// <param name="result">The rows.</param>
    internal WorfRows(SpiResult result)
    {
        _result = result;
        Columns = [.. result.Columns.Select(column => (column.Name, column.TypeOid))];
        RowsAffected = result.RowsAffected;
    }

    /// <summary>
    /// Initializes the rows that SPI returned to worf directly, as a query with exact bytes returns them.
    /// </summary>
    /// <param name="columns">The query's columns.</param>
    /// <param name="tuples">The rows.</param>
    /// <param name="rowsAffected">The number of rows the query processed.</param>
    internal WorfRows(IReadOnlyList<(string Name, uint TypeOid)> columns, List<PgHeapTuple> tuples, long rowsAffected)
    {
        _tuples = tuples;
        Columns = columns;
        RowsAffected = rowsAffected;
    }

    /// <summary>
    /// Gets the query's columns.
    /// </summary>
    internal IReadOnlyList<(string Name, uint TypeOid)> Columns { get; }

    /// <summary>
    /// Gets the number of rows the query processed.
    /// </summary>
    internal long RowsAffected { get; }

    /// <inheritdoc/>
    public int Count => _result?.Count ?? _tuples!.Count;

    /// <inheritdoc/>
    public WorfRow this[int index] => _result is not null ? new WorfRow(_result[index], Columns) : new WorfRow(_tuples![index], Columns);

    /// <inheritdoc/>
    public IEnumerator<WorfRow> GetEnumerator()
    {
        for (int i = 0; i < Count; i++)
        {
            yield return this[i];
        }
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
