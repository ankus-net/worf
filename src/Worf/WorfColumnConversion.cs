using Ankus.Postgres;

namespace Worf;

/// <summary>
/// How a remote column's values, sent in another type than their local column's, convert to it: read with the type
/// they were sent in, then converted as an assignment would, as <c>ALTER COLUMN TYPE</c> without <c>USING</c> converts
/// a column's values. Text is the way between them only where PostgreSQL's own conversion goes through it.
/// </summary>
internal sealed unsafe class WorfColumnConversion
{
    // The local column's name and the name of the type the values are sent in, for values this node reads.
    private readonly string? _column;
    private readonly string? _sourceType;

    // The error that values this node can't read fail with.
    private readonly string? _sqlState;
    private readonly string? _error;

    /// <summary>
    /// Initializes a new instance of the <see cref="WorfColumnConversion"/> class for values this node reads.
    /// </summary>
    /// <param name="column">The local column's name.</param>
    /// <param name="sourceType">The name of the type the values are sent in.</param>
    /// <param name="readType">The local type they're read with.</param>
    internal WorfColumnConversion(string column, string sourceType, uint readType)
    {
        _column = column;
        _sourceType = sourceType;
        ReadType = readType;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="WorfColumnConversion"/> class for values of a type this node
    /// lacks, or defines differently, which fail with an error.
    /// </summary>
    /// <param name="sqlState">The error's SQLSTATE.</param>
    /// <param name="error">The error's message.</param>
    internal WorfColumnConversion(string sqlState, string error)
    {
        _sqlState = sqlState;
        _error = error;
    }

    /// <summary>
    /// Gets the local type the values are read with, or zero when this node can't read them.
    /// </summary>
    public uint ReadType { get; }

    /// <summary>
    /// Reads a value with a type, without a domain's constraints.
    /// </summary>
    /// <param name="type">The type, which isn't a domain.</param>
    /// <param name="kind">The value's form: <c>i</c> for PostgreSQL's internal one, <c>b</c> for binary, <c>t</c> for
    /// text.</param>
    /// <param name="data">Its bytes, without the text's terminating zero.</param>
    /// <returns>The datum.</returns>
    internal static ulong Read(uint type, char kind, byte[] data)
    {
        uint typioparam;
        fixed (byte* bytes = data)
        {
            switch (kind)
            {
                case 'i':
                {
                    short typlen;
                    bool typbyval;
                    NativeMethods.get_typlenbyval(type, &typlen, &typbyval);
                    if (typbyval)
                    {
                        return ProtoNative.FetchAttByval((sbyte*)bytes, data.Length);
                    }

                    // A copy is aligned, as the type's functions expect.
                    byte* copy = (byte*)NativeMethods.palloc((ulong)data.Length);
                    data.CopyTo(new Span<byte>(copy, data.Length));
                    return (ulong)copy;
                }

                case 'b':
                {
                    uint typreceive;
                    NativeMethods.getTypeBinaryInputInfo(type, &typreceive, &typioparam);
                    StringInfoData buf;
                    NativeMethods.initStringInfo(&buf);
                    Worf.AppendBinaryStringInfo(&buf, bytes, data.Length);
                    ulong value = NativeMethods.OidReceiveFunctionCall(typreceive, &buf, typioparam, -1);
                    if (Worf.UnreadLength(&buf) != 0)
                    {
                        throw new PgException(PgSqlStates.InvalidBinaryRepresentation, "incorrect binary data format");
                    }

                    return value;
                }

                default:
                {
                    uint typinput;
                    NativeMethods.getTypeInputInfo(type, &typinput, &typioparam);
                    byte* text = (byte*)NativeMethods.palloc((ulong)data.Length + 1);
                    data.CopyTo(new Span<byte>(text, data.Length));
                    text[data.Length] = 0;
                    return NativeMethods.OidInputFunctionCall(typinput, (sbyte*)text, typioparam, -1);
                }
            }
        }
    }

    /// <summary>
    /// Converts a value to the local column's type, with the coercion <c>ALTER COLUMN TYPE</c> builds and evaluates.
    /// </summary>
    /// <param name="kind">The value's form, as <see cref="Read"/> takes it.</param>
    /// <param name="data">Its bytes.</param>
    /// <param name="att">The local column.</param>
    /// <param name="isnull">Whether the result is null.</param>
    /// <returns>The datum.</returns>
    internal ulong Convert(char kind, byte[] data, FormData_pg_attribute* att, out bool isnull)
    {
        if (ReadType == 0)
        {
            throw new PgException(_sqlState!, _error!);
        }

        ulong value = Read(ReadType, kind, data);

        short typlen;
        bool typbyval;
        NativeMethods.get_typlenbyval(ReadType, &typlen, &typbyval);
        Const* source = NativeMethods.makeConst(ReadType, -1, NativeMethods.get_typcollation(ReadType), typlen, value, false, typbyval);
        ParseState* pstate = NativeMethods.make_parsestate(null);
        Node* coerced = NativeMethods.coerce_to_target_type(pstate, (Node*)source, ReadType, att->atttypid, att->atttypmod,
            CoercionContext.COERCION_ASSIGNMENT, CoercionForm.COERCE_IMPLICIT_CAST, -1);
        if (coerced is null)
        {
            throw new PgException(PgSqlStates.DatatypeMismatch,
                $"column \"{_column}\" is of type {Worf.FromServer(NativeMethods.format_type_be(att->atttypid))} but the provider's value is of type {_sourceType}");
        }

        NativeMethods.assign_expr_collations(pstate, coerced);
        NativeMethods.free_parsestate(pstate);

        ExprState* state = NativeMethods.ExecInitExpr((Expr*)coerced, null);
        ExprContext* econtext = NativeMethods.CreateStandaloneExprContext();
        bool resultnull;
        ulong result = NativeMethods.ExecEvalExpr(state, econtext, &resultnull);
        NativeMethods.FreeExprContext(econtext, true);
        isnull = resultnull;
        return result;
    }
}
