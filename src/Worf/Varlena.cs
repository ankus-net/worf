using System.Runtime.CompilerServices;

namespace Worf;

/// <summary>
/// The varlena header layout of little-endian servers, from varatt.h.
/// </summary>
internal static unsafe class Varlena
{
    /// <summary>
    /// <c>VARHDRSZ</c>.
    /// </summary>
    internal const int Hdrsz = 4;

    private const byte VartagIndirect = 1;
    private const byte VartagExpandedRo = 2;
    private const byte VartagExpandedRw = 3;
    private const byte VartagOndisk = 18;

    /// <summary>
    /// <c>VARATT_IS_EXTERNAL_ONDISK</c>.
    /// </summary>
    internal static bool IsExternalOndisk(ulong datum) => IsExternal(datum) && ((byte*)datum)[1] == VartagOndisk;

    /// <summary>
    /// <c>VARATT_IS_EXTERNAL_INDIRECT</c>.
    /// </summary>
    internal static bool IsExternalIndirect(ulong datum) => IsExternal(datum) && ((byte*)datum)[1] == VartagIndirect;

    /// <summary>
    /// <c>VARSIZE_ANY</c>.
    /// </summary>
    internal static int SizeAny(byte* ptr)
    {
        if (ptr[0] == 0x01)
        {
            byte tag = ptr[1];
            int tagSize = tag switch
            {
                VartagIndirect => sizeof(nint),
                VartagExpandedRo or VartagExpandedRw => sizeof(nint),
                VartagOndisk => 16,
                _ => 0,
            };
            return 2 + tagSize;
        }

        if ((ptr[0] & 0x01) == 0x01)
        {
            return (ptr[0] >> 1) & 0x7F;
        }

        return Size(ptr);
    }

    /// <summary>
    /// <c>VARSIZE</c> of a four-byte header.
    /// </summary>
    internal static int Size(byte* ptr) => (int)((Unsafe.ReadUnaligned<uint>(ptr) >> 2) & 0x3FFFFFFF);

    /// <summary>
    /// Checks whether bytes are a whole varlena with its value inline, as a value in PostgreSQL's internal format has
    /// to be: a one-byte header, or a four-byte header with room for a compressed value's own header, whose size is
    /// the number of bytes.
    /// </summary>
    /// <param name="ptr">The bytes.</param>
    /// <param name="len">How many there are.</param>
    /// <returns>Whether they are.</returns>
    internal static bool IsInline(byte* ptr, int len)
    {
        if (len < 1 || ptr[0] == 0x01)
        {
            return false;
        }

        if ((ptr[0] & 0x01) == 0x01)
        {
            return ((ptr[0] >> 1) & 0x7F) == len;
        }

        // A compressed value's header also holds its size and method.
        return len >= ((ptr[0] & 0x03) == 0x02 ? 2 * Hdrsz : Hdrsz) && Size(ptr) == len;
    }

    private static bool IsExternal(ulong datum) => ((byte*)datum)[0] == 0x01;
}
