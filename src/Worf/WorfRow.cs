using Ankus.Postgres;

namespace Worf;

/// <summary>
/// A row a query of worf's returned, as <see cref="SpiRow"/> holds one, whose text cells read exactly: the bytes of a
/// SQL_ASCII name that aren't valid UTF-8, which Ankus refuses to read as a string, read as the unpaired surrogates
/// <see cref="Worf.FromServer(ReadOnlySpan{byte})"/> reads them as.
/// </summary>
internal readonly struct WorfRow
{
    private const uint NameOid = 19;
    private const uint TextOid = 25;
    private const uint BpcharOid = 1042;
    private const uint VarcharOid = 1043;
    private const uint NameArrayOid = 1003;
    private const uint TextArrayOid = 1009;
    private const uint BpcharArrayOid = 1014;
    private const uint VarcharArrayOid = 1015;

    private readonly SpiRow? _row;
    private readonly PgHeapTuple? _tuple;
    private readonly IReadOnlyList<(string Name, uint TypeOid)> _columns;

    /// <summary>
    /// Initializes a row from one Ankus read.
    /// </summary>
    /// <param name="row">The row.</param>
    /// <param name="columns">The query's columns.</param>
    internal WorfRow(SpiRow row, IReadOnlyList<(string Name, uint TypeOid)> columns)
    {
        _row = row;
        _columns = columns;
    }

    /// <summary>
    /// Initializes a row from one that SPI returned to worf directly, as a query with exact bytes returns them.
    /// </summary>
    /// <param name="tuple">The row.</param>
    /// <param name="columns">The query's columns.</param>
    internal WorfRow(PgHeapTuple tuple, IReadOnlyList<(string Name, uint TypeOid)> columns)
    {
        _tuple = tuple;
        _columns = columns;
    }

    /// <summary>
    /// Gets the number of cells.
    /// </summary>
    internal int Count => _columns.Count;

    /// <summary>
    /// Gets a cell by its zero-based ordinal, or null for SQL NULL.
    /// </summary>
    /// <param name="ordinal">The ordinal.</param>
    /// <returns>The cell.</returns>
    internal object? this[int ordinal]
    {
        get
        {
            try
            {
                return _row is not null ? _row[ordinal] : _tuple![ordinal];
            }
            catch (PgException error) when (error.SqlState == PgSqlStates.CharacterNotInRepertoire)
            {
                object? exact = Exact(ordinal);
                if (exact is null)
                {
                    throw;
                }

                return exact;
            }
        }
    }

    /// <summary>
    /// Gets a cell by its column's name, or null for SQL NULL.
    /// </summary>
    /// <param name="name">The column's name.</param>
    /// <returns>The cell.</returns>
    internal object? this[string name] => this[GetOrdinal(name)];

    /// <summary>
    /// Gets a typed cell by its zero-based ordinal, as <see cref="SpiRow.Get{T}(int)"/> does.
    /// </summary>
    /// <typeparam name="T">The cell's managed type.</typeparam>
    /// <param name="ordinal">The ordinal.</param>
    /// <returns>The cell.</returns>
    internal T Get<[PgConvertible] T>(int ordinal)
    {
        try
        {
            return _row is not null ? _row.Get<T>(ordinal) : _tuple!.Get<T>(ordinal);
        }
        catch (PgException error) when (error.SqlState == PgSqlStates.CharacterNotInRepertoire &&
            (typeof(T) == typeof(string) || typeof(T) == typeof(string[])))
        {
            if (Exact(ordinal) is not T exact)
            {
                throw;
            }

            return exact;
        }
    }

    /// <summary>
    /// Gets a typed cell by its column's name, as <see cref="SpiRow.Get{T}(string)"/> does.
    /// </summary>
    /// <typeparam name="T">The cell's managed type.</typeparam>
    /// <param name="name">The column's name.</param>
    /// <returns>The cell.</returns>
    internal T Get<[PgConvertible] T>(string name) => Get<T>(GetOrdinal(name));

    /// <summary>
    /// Gets a cell's type.
    /// </summary>
    /// <param name="ordinal">The ordinal.</param>
    /// <returns>The type's OID.</returns>
    internal uint GetTypeOid(int ordinal) => _row?.GetTypeOid(ordinal) ?? _columns[ordinal].TypeOid;

    /// <summary>
    /// Finds the first column with a name.
    /// </summary>
    private int GetOrdinal(string name)
    {
        for (int i = 0; i < _columns.Count; i++)
        {
            if (_columns[i].Name == name)
            {
                return i;
            }
        }

        throw new ArgumentOutOfRangeException(nameof(name), $"No column is named \"{name}\".");
    }

    /// <summary>
    /// Reads a text cell, or an array of them, from its bytes, or null for another type.
    /// </summary>
    private unsafe object? Exact(int ordinal)
    {
        PgDatum datum = _row is not null ? _row.Get<PgDatum>(ordinal) : _tuple!.Get<PgDatum>(ordinal);
        if (datum.IsNull)
        {
            return null;
        }

        nuint bits = datum.DangerousGetBits();
        switch (datum.TypeOid)
        {
            case NameOid:
                return Worf.FromServer((sbyte*)bits);
            case TextOid or BpcharOid or VarcharOid:
                return Worf.FromServer(NativeMethods.text_to_cstring((varlena*)bits));
            case NameArrayOid or TextArrayOid or BpcharArrayOid or VarcharArrayOid:
                uint element = datum.TypeOid switch
                {
                    NameArrayOid => NameOid,
                    TextArrayOid => TextOid,
                    BpcharArrayOid => BpcharOid,
                    _ => VarcharOid,
                };
                short length;
                bool byValue;
                sbyte align;
                NativeMethods.get_typlenbyvalalign(element, &length, &byValue, &align);
                ulong* elements;
                bool* nulls;
                int count;
                NativeMethods.deconstruct_array((ArrayType*)NativeMethods.pg_detoast_datum((varlena*)bits), element, length, byValue, align,
                    &elements, &nulls, &count);
                string?[] values = new string?[count];
                for (int i = 0; i < count; i++)
                {
                    values[i] = nulls[i] ? null
                        : element == NameOid ? Worf.FromServer((sbyte*)elements[i])
                        : Worf.FromServer(NativeMethods.text_to_cstring((varlena*)elements[i]));
                }

                return values;
            default:
                return null;
        }
    }
}
