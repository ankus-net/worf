namespace Worf;

/// <summary>
/// Keeps the plans of the SQL that worf runs again and again, as PL/pgSQL keeps a function's statements' plans, so that
/// a worker's tick, or a statement a guard checks, doesn't parse, analyze and plan the same SQL each time. A statement
/// is kept from its second run, with parameters of the same types, so that SQL made for one use takes no room, and the
/// least recently used goes once too many are kept. PostgreSQL plans a kept statement again when a table or function
/// it uses, or the search path, changes, as it does a prepared statement's.
/// </summary>
internal static class PlanCache
{
    private const int Capacity = 256;
    private const int SeenCapacity = 4096;

    private static readonly Dictionary<string, LinkedListNode<(string Key, SpiPreparedStatement Statement)>> s_plans = [];
    private static readonly LinkedList<(string Key, SpiPreparedStatement Statement)> s_used = [];
    private static readonly HashSet<string> s_seen = [];

    /// <summary>
    /// Gets the kept plan of SQL run before with parameters of the same types, keeping it at its second run, or null
    /// for SQL run once, or that holds several statements, whose plans made in advance would analyze each before the
    /// ones ahead of it had run.
    /// </summary>
    /// <param name="commandText">The SQL.</param>
    /// <param name="parameters">Its parameters.</param>
    /// <returns>The kept statement, or null to run the SQL once.</returns>
    internal static SpiPreparedStatement? Get(string commandText, ReadOnlySpan<SpiParameter> parameters)
    {
        if (commandText.Contains(';', StringComparison.Ordinal))
        {
            return null;
        }

        uint[] types = new uint[parameters.Length];
        for (int i = 0; i < parameters.Length; i++)
        {
            types[i] = parameters[i].TypeOid;
        }

        string key = string.Join(',', types) + ":" + commandText;
        if (s_plans.TryGetValue(key, out LinkedListNode<(string Key, SpiPreparedStatement Statement)>? kept))
        {
            s_used.Remove(kept);
            s_used.AddFirst(kept);
            return kept.Value.Statement;
        }

        if (s_seen.Add(key))
        {
            if (s_seen.Count > SeenCapacity)
            {
                s_seen.Clear();
                _ = s_seen.Add(key);
            }

            return null;
        }

        SpiPreparedStatement statement = Spi.PrepareWithTypeOids(commandText, types).Keep();
        _ = s_seen.Remove(key);
        s_plans[key] = s_used.AddFirst((key, statement));
        if (s_plans.Count > Capacity)
        {
            (string oldest, SpiPreparedStatement evicted) = s_used.Last!.Value;
            s_used.RemoveLast();
            _ = s_plans.Remove(oldest);
            evicted.Dispose();
        }

        return statement;
    }
}
