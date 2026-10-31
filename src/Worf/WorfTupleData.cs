using Ankus.Postgres;

namespace Worf;

/// <summary>
/// A tuple read from the stream, in the local relation's column order.
/// </summary>
internal sealed class WorfTupleData
{
    /// <summary>
    /// Initializes a new instance of the <see cref="WorfTupleData"/> class with every column null and unchanged.
    /// </summary>
    /// <param name="natts">The local relation's number of columns.</param>
    internal WorfTupleData(int natts)
    {
        Kinds = new char[natts];
        Data = new byte[]?[natts];
        Values = new ulong[natts];
        Nulls = new bool[natts];
        Changed = new bool[natts];
        Converted = new bool[natts];
        Array.Fill(Nulls, true);
    }

    /// <summary>
    /// Gets each column's representation on the wire, or zero when the column wasn't sent.
    /// </summary>
    public char[] Kinds { get; }

    /// <summary>
    /// Gets each text or binary column's bytes, without the text's terminating zero.
    /// </summary>
    public byte[]?[] Data { get; }

    /// <summary>
    /// Gets each column's datum, once converted.
    /// </summary>
    public ulong[] Values { get; }

    /// <summary>
    /// Gets whether each column is null.
    /// </summary>
    public bool[] Nulls { get; }

    /// <summary>
    /// Gets whether each column was sent.
    /// </summary>
    public bool[] Changed { get; }

    /// <summary>
    /// Gets whether each column's datum has been converted.
    /// </summary>
    public bool[] Converted { get; }

    /// <summary>
    /// Gets or sets how each column sent in binary in another type than its own converts, or null when none was.
    /// </summary>
    public WorfColumnConversion?[]? Conversions { get; set; }

    /// <summary>
    /// Converts a column sent in binary in another type than its own to its datum, once.
    /// </summary>
    /// <param name="attid">The column, which has a conversion.</param>
    internal void ConvertFromSentType(int attid)
    {
        if (!Converted[attid])
        {
            Values[attid] = Conversions![attid]!.Convert(Kinds[attid], Data[attid]!, out Nulls[attid]);
            Converted[attid] = true;
        }
    }

    /// <summary>
    /// Converts every sent column to its datum with the column type's input or receive function, as the native
    /// protocol's reader does, or for a value sent in binary in another type, from that type.
    /// </summary>
    /// <param name="desc">The local relation's descriptor.</param>
    internal unsafe void ConvertValues(TupleDescData* desc)
    {
        for (int attid = 0; attid < Kinds.Length; attid++)
        {
            if (Converted[attid] || Data[attid] is null)
            {
                continue;
            }

            if (Conversions?[attid] is not null)
            {
                ConvertFromSentType(attid);
                continue;
            }

            FormData_pg_attribute* att = NativeMethods.TupleDescAttr(desc, attid);
            byte[] data = Data[attid]!;
            uint typioparam;

            if (Kinds[attid] == 'b')
            {
                uint typreceive;

                NativeMethods.getTypeBinaryInputInfo(att->atttypid, &typreceive, &typioparam);

                // create StringInfo pointing into the bigger buffer
                StringInfoData buf;
                NativeMethods.initStringInfo(&buf);
                fixed (byte* bytes = data)
                {
                    Worf.AppendBinaryStringInfo(&buf, bytes, data.Length);
                }

                Values[attid] = NativeMethods.OidReceiveFunctionCall(typreceive, &buf, typioparam, att->atttypmod);

                if (Worf.UnreadLength(&buf) != 0)
                {
                    throw new PgException(PgSqlStates.InvalidBinaryRepresentation, "incorrect binary data format");
                }
            }
            else
            {
                uint typinput;

                NativeMethods.getTypeInputInfo(att->atttypid, &typinput, &typioparam);

                // and data
                byte* text = (byte*)NativeMethods.palloc((ulong)data.Length + 1);
                data.CopyTo(new Span<byte>(text, data.Length));
                text[data.Length] = 0;
                Values[attid] = NativeMethods.OidInputFunctionCall(typinput, (sbyte*)text, typioparam, att->atttypmod);
            }

            Converted[attid] = true;
        }
    }
}
