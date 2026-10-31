using Ankus.Postgres;

namespace Worf;

/// <summary>
/// How a remote column's values, sent in another type than their local column's, convert to it: read with the type
/// they were sent in, then converted as an assignment would, as <c>ALTER COLUMN TYPE</c> without <c>USING</c> converts
/// a column's values, or else through text, which the column type's input reads with its type modifier, as it reads
/// text. A value sent in binary in a type this node lacks or defines differently can't be read. The conversion is
/// compiled for its first value and kept, in its own memory context, until it's released.
/// </summary>
internal sealed unsafe class WorfColumnConversion
{
    /// <summary>
    /// The OID of text, whose values an enum's labels are read as.
    /// </summary>
    internal const uint TextOid = 25;

    // The local column's name and the name of the type the values are sent in, for errors.
    private readonly string _column;
    private readonly string _sourceType;

    // The local column's type and type modifier.
    private readonly uint _targetType;
    private readonly int _targetTypmod;

    // What the conversion keeps once compiled: its memory context, the read type's length, its receive and input
    // functions and their parameter, the compiled conversion and the context it's evaluated in, or, for values with no
    // assignment cast, the read type's output function and the column type's input function and its parameter, and
    // the column type's send function, or null when it has none.
    private MemoryContextData* _context;
    private short _typlen;
    private bool _typbyval;
    private FmgrInfo* _receive;
    private FmgrInfo* _input;
    private uint _typioparam;
    private ExprState* _state;
    private ExprContext* _econtext;
    private FmgrInfo* _output;
    private FmgrInfo* _columnInput;
    private uint _columnTypioparam;
    private FmgrInfo* _send;

    /// <summary>
    /// Initializes a new instance of the <see cref="WorfColumnConversion"/> class.
    /// </summary>
    /// <param name="column">The local column's name.</param>
    /// <param name="sourceType">The name of the type the values are sent in.</param>
    /// <param name="readType">The local type they're read with, which isn't a domain, or zero when this node lacks
    /// the type or defines it differently.</param>
    /// <param name="targetType">The local column's type.</param>
    /// <param name="targetTypmod">The local column's type modifier.</param>
    internal WorfColumnConversion(string column, string sourceType, uint readType, uint targetType, int targetTypmod)
    {
        _column = column;
        _sourceType = sourceType;
        ReadType = readType;
        _targetType = targetType;
        _targetTypmod = targetTypmod;
    }

    /// <summary>
    /// Gets the local type the values are read with, or zero when this node can't read them in binary.
    /// </summary>
    public uint ReadType { get; }

    /// <summary>
    /// Gets whether the values have no assignment cast to the column's type, and go through text. Compiles the
    /// conversion of values this node reads.
    /// </summary>
    public bool ThroughText
    {
        get
        {
            Compile();
            return _state is null;
        }
    }

    /// <summary>
    /// Reads a value sent in binary with a type, without a domain's constraints.
    /// </summary>
    /// <param name="type">The type, which isn't a domain.</param>
    /// <param name="kind">The value's form: <c>i</c> for PostgreSQL's internal one, <c>b</c> for binary.</param>
    /// <param name="data">Its bytes.</param>
    /// <returns>The datum.</returns>
    internal static ulong Read(uint type, char kind, byte[] data)
    {
        fixed (byte* bytes = data)
        {
            if (kind == 'i')
            {
                short typlen;
                bool typbyval;
                NativeMethods.get_typlenbyval(type, &typlen, &typbyval);
                return ReadInternal(type, typlen, typbyval, bytes, data.Length);
            }

            uint typreceive;
            uint typioparam;
            NativeMethods.getTypeBinaryInputInfo(type, &typreceive, &typioparam);
            StringInfoData buf;
            NativeMethods.initStringInfo(&buf);
            Worf.AppendBinaryStringInfo(&buf, bytes, data.Length);
            ulong value = NativeMethods.OidReceiveFunctionCall(typreceive, &buf, typioparam, -1);
            CheckReadToEnd(&buf);
            return value;
        }
    }

    /// <summary>
    /// Gets a datum from its bytes in PostgreSQL's internal format, which have to be as long as the type's values,
    /// or a varlena's or C string's own length. A value passed by reference is copied, aligned.
    /// </summary>
    /// <param name="type">The type, for the error.</param>
    /// <param name="typlen">The type's length.</param>
    /// <param name="typbyval">Whether the type's values are passed by value.</param>
    /// <param name="data">The bytes.</param>
    /// <param name="len">How many there are.</param>
    /// <returns>The datum.</returns>
    internal static ulong ReadInternal(uint type, short typlen, bool typbyval, byte* data, int len)
    {
        bool valid = typlen switch
        {
            > 0 => len == typlen && (!typbyval || typlen is 1 or 2 or 4 or 8),
            -1 => Varlena.IsInline(data, len),
            -2 => len > 0 && new ReadOnlySpan<byte>(data, len).IndexOf((byte)0) == len - 1,
            _ => false,
        };
        if (!valid)
        {
            throw new PgException(PgSqlStates.InvalidBinaryRepresentation,
                $"incorrect internal data format of {len} bytes for type {Worf.FromServer(NativeMethods.format_type_be(type))}");
        }

        if (typbyval)
        {
            return ProtoNative.FetchAttByval((sbyte*)data, len);
        }

        byte* copy = (byte*)NativeMethods.palloc((ulong)len);
        new ReadOnlySpan<byte>(data, len).CopyTo(new Span<byte>(copy, len));
        return (ulong)copy;
    }

    /// <summary>
    /// Converts a value to the local column's type.
    /// </summary>
    /// <param name="kind">The value's form: <c>i</c> for PostgreSQL's internal one, <c>b</c> for binary, <c>t</c> for
    /// text.</param>
    /// <param name="data">Its bytes, without the text's terminating zero.</param>
    /// <param name="isnull">Whether the result is null.</param>
    /// <returns>The datum, in the current memory context.</returns>
    internal ulong Convert(char kind, byte[] data, out bool isnull)
    {
        if (ReadType == 0)
        {
            string targetType = Worf.FromServer(NativeMethods.format_type_be(_targetType));
            throw new PgException(PgSqlStates.DatatypeMismatch,
                $"column \"{_column}\" is of type {targetType} but the provider's value is of type {_sourceType}, which this node lacks or defines differently");
        }

        Compile();

        ulong value;
        fixed (byte* bytes = data)
        {
            if (kind == 't')
            {
                // Text with no assignment cast is the text the column type's input reads.
                sbyte* text = CString(bytes, data.Length);
                if (_state is null)
                {
                    isnull = false;
                    return NativeMethods.InputFunctionCall(_columnInput, text, _columnTypioparam, _targetTypmod);
                }

                value = NativeMethods.InputFunctionCall(_input, text, _typioparam, -1);
            }
            else if (kind == 'i')
            {
                value = ReadInternal(ReadType, _typlen, _typbyval, bytes, data.Length);
            }
            else if (_receive is null)
            {
                throw new PgException(PgSqlStates.UndefinedFunction, $"no binary input function available for type {_sourceType}");
            }
            else
            {
                StringInfoData buf;
                NativeMethods.initStringInfo(&buf);
                Worf.AppendBinaryStringInfo(&buf, bytes, data.Length);
                value = NativeMethods.ReceiveFunctionCall(_receive, &buf, _typioparam, -1);
                CheckReadToEnd(&buf);
            }
        }

        if (_state is null)
        {
            isnull = false;
            return NativeMethods.InputFunctionCall(_columnInput, NativeMethods.OutputFunctionCall(_output, value),
                _columnTypioparam, _targetTypmod);
        }

        _econtext->caseValue_datum = value;
        _econtext->caseValue_isNull = false;
        return Executor.ExecEvalExpr(_state, _econtext, out isnull);
    }

    /// <summary>
    /// Gets a converted value in the binary form the local column type's receive function reads.
    /// </summary>
    /// <param name="value">The value, which isn't null.</param>
    /// <returns>The bytes, after a four-byte varlena header, or null when the type has no send function.</returns>
    internal varlena* Send(ulong value) => _send is null ? null : NativeMethods.SendFunctionCall(_send, value);

    /// <summary>
    /// Frees what the conversion keeps. A value converted afterwards compiles it again.
    /// </summary>
    internal void Release()
    {
        if (_context is not null)
        {
            NativeMethods.MemoryContextDelete(_context);
        }

        _context = null;
        _receive = null;
        _input = null;
        _state = null;
        _econtext = null;
        _output = null;
        _columnInput = null;
        _send = null;
    }

    private static void CheckReadToEnd(StringInfoData* buf)
    {
        if (Worf.UnreadLength(buf) != 0)
        {
            throw new PgException(PgSqlStates.InvalidBinaryRepresentation, "incorrect binary data format");
        }
    }

    /// <summary>
    /// Copies text into the current memory context with its terminating zero.
    /// </summary>
    private static sbyte* CString(byte* data, int len)
    {
        byte* text = (byte*)NativeMethods.palloc((ulong)len + 1);
        new ReadOnlySpan<byte>(data, len).CopyTo(new Span<byte>(text, len));
        text[len] = 0;
        return (sbyte*)text;
    }

    /// <summary>
    /// Gets a function's call information, kept in a memory context.
    /// </summary>
    private static FmgrInfo* FunctionInfo(uint function, MemoryContextData* context)
    {
        FmgrInfo* info = (FmgrInfo*)NativeMethods.palloc0((ulong)sizeof(FmgrInfo));
        NativeMethods.fmgr_info_cxt(function, info, context);
        return info;
    }

    /// <summary>
    /// Compiles the conversion once: the coercion <c>ALTER COLUMN TYPE</c> builds, over a placeholder for the value
    /// read, planned and initialized, or, when there's none, the functions that take the value through text, with the
    /// functions that read and send values.
    /// </summary>
    private void Compile()
    {
        if (_context is not null)
        {
            return;
        }

        MemoryContextData* context;
        fixed (byte* name = "worf column conversion\0"u8)
        {
            // The name is static, as a memory context's has to be.
            context = NativeMethods.AllocSetContextCreateInternal(NativeGlobals.CacheMemoryContext, (sbyte*)name, 0,
                1024, 8 * 1024);
        }

        MemoryContextData* oldcontext = Worf.MemoryContextSwitchTo(context);
        bool compiled = false;
        try
        {
            HeapTupleData* typtup = NativeMethods.typeidType(ReadType);
            FormData_pg_type* typclass = (FormData_pg_type*)NativeMethods.GETSTRUCT(typtup);
            _typlen = typclass->typlen;
            _typbyval = typclass->typbyval;
            uint typreceive = typclass->typreceive;
            NativeMethods.ReleaseSysCache(typtup);

            uint typinput;
            uint typioparam;
            NativeMethods.getTypeInputInfo(ReadType, &typinput, &typioparam);
            FmgrInfo* receive = typreceive == 0 ? null : FunctionInfo(typreceive, context);
            FmgrInfo* input = FunctionInfo(typinput, context);

            CaseTestExpr* source = (CaseTestExpr*)NativeMethods.palloc0((ulong)sizeof(CaseTestExpr));
            source->xpr.type = NodeTag.T_CaseTestExpr;
            source->typeId = ReadType;
            source->typeMod = -1;
            source->collation = NativeMethods.get_typcollation(ReadType);

            Node* coerced = Coerce((Node*)source);
            ExprState* state = null;
            ExprContext* econtext = null;
            FmgrInfo* output = null;
            FmgrInfo* columnInput = null;
            uint columnTypioparam = 0;
            if (coerced is not null)
            {
                state = NativeMethods.ExecInitExpr(NativeMethods.expression_planner((Expr*)coerced), null);
                econtext = NativeMethods.CreateStandaloneExprContext();
            }
            else
            {
                uint typoutput;
                bool typisvarlena;
                NativeMethods.getTypeOutputInfo(ReadType, &typoutput, &typisvarlena);
                output = FunctionInfo(typoutput, context);

                uint columnTypinput;
                NativeMethods.getTypeInputInfo(_targetType, &columnTypinput, &columnTypioparam);
                columnInput = FunctionInfo(columnTypinput, context);
            }

            typtup = NativeMethods.typeidType(_targetType);
            uint typsend = ((FormData_pg_type*)NativeMethods.GETSTRUCT(typtup))->typsend;
            NativeMethods.ReleaseSysCache(typtup);
            FmgrInfo* send = typsend == 0 ? null : FunctionInfo(typsend, context);

            _context = context;
            _receive = receive;
            _input = input;
            _typioparam = typioparam;
            _state = state;
            _econtext = econtext;
            _output = output;
            _columnInput = columnInput;
            _columnTypioparam = columnTypioparam;
            _send = send;
            compiled = true;
        }
        finally
        {
            _ = Worf.MemoryContextSwitchTo(oldcontext);
            if (!compiled)
            {
                NativeMethods.MemoryContextDelete(context);
            }
        }
    }

    /// <summary>
    /// Builds the assignment coercion of a value of the read type to the local column's type, with its collations, or
    /// null when there's none.
    /// </summary>
    private Node* Coerce(Node* source)
    {
        ParseState* pstate = NativeMethods.make_parsestate(null);
        Node* coerced = NativeMethods.coerce_to_target_type(pstate, source, ReadType, _targetType, _targetTypmod,
            CoercionContext.COERCION_ASSIGNMENT, CoercionForm.COERCE_IMPLICIT_CAST, -1);
        if (coerced is not null)
        {
            NativeMethods.assign_expr_collations(pstate, coerced);
        }

        NativeMethods.free_parsestate(pstate);
        return coerced;
    }
}
