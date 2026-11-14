namespace Worf;

/// <summary>
/// The rows a node's message to a neighbor gives of its sequences, each part read for every sequence at once, by part and
/// sequence.
/// </summary>
internal sealed class WorfNeighborRows
{
    private readonly Dictionary<(string Part, uint Seqoid), List<WorfRow>> _rows = [];

    /// <summary>
    /// Gets the Snowflake node number sequence, or zero.
    /// </summary>
    internal uint Snowflake { get; init; }

    /// <summary>
    /// Gets a part's rows of a sequence.
    /// </summary>
    /// <param name="part">The part.</param>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The rows, or none.</returns>
    internal IReadOnlyList<WorfRow> this[string part, uint seqoid] => _rows.TryGetValue((part, seqoid), out List<WorfRow>? found) ? found : [];

    /// <summary>
    /// Adds a part's rows, each of which names its sequence in a column.
    /// </summary>
    /// <param name="part">The part.</param>
    /// <param name="rows">The rows.</param>
    /// <param name="column">The column that names each row's sequence.</param>
    internal void Add(string part, WorfRows rows, int column)
    {
        foreach (WorfRow row in rows)
        {
            (string, uint) key = (part, row.Get<uint>(column));
            if (!_rows.TryGetValue(key, out List<WorfRow>? found))
            {
                found = [];
                _rows[key] = found;
            }

            found.Add(row);
        }
    }
}
